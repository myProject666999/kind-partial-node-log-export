/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package logs

import (
	"io"
	"os"
	"path/filepath"
	"sync"
)

// partialSuffix is appended to the name of every log artifact that was not
// written completely (step failed, timed out, or was still being written when
// the export ended). Tooling must never treat a *.partial path as complete.
const partialSuffix = ".partial"

// tailWriter keeps only the last max bytes of everything written to it.
//
// It is bounded: at most max bytes are ever retained, regardless of how much
// is written, so multi-gigabyte logs never end up in memory. A write that
// arrives after freeze is discarded, which makes it safe for a timed out
// command to keep writing while the step result is being finalized.
type tailWriter struct {
	mu     sync.Mutex
	max    int
	buf    []byte // logical contents are buf[start:]+buf[:start]
	start  int
	total  int64
	frozen bool
}

func newTailWriter(max int) *tailWriter {
	return &tailWriter{max: max}
}

// Write implements io.Writer. It never returns an error; the only loss is
// truncation, which is reported through Truncated.
func (t *tailWriter) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := len(p)
	if t.frozen || n == 0 {
		return n, nil
	}
	t.total += int64(n)

	// Fill the buffer up to max without ever growing its capacity past max.
	if len(t.buf) < t.max {
		space := t.max - len(t.buf)
		if space > len(p) {
			space = len(p)
		}
		t.appendBounded(p[:space])
		p = p[space:]
	}
	// Everything else overwrites the oldest bytes (ring buffer).
	for len(p) > 0 {
		space := t.max - t.start
		if space > len(p) {
			space = len(p)
		}
		copy(t.buf[t.start:t.start+space], p[:space])
		t.start = (t.start + space) % t.max
		p = p[space:]
	}
	return n, nil
}

// appendBounded appends p to buf, growing the capacity geometrically but never
// above max. This keeps the writer's memory bounded by max even for a single
// very large write.
func (t *tailWriter) appendBounded(p []byte) {
	need := len(t.buf) + len(p)
	if need > cap(t.buf) {
		newCap := 2 * cap(t.buf)
		if newCap < 4096 {
			newCap = 4096
		}
		if newCap < need {
			newCap = need
		}
		if newCap > t.max {
			newCap = t.max
		}
		buf := make([]byte, len(t.buf), newCap)
		copy(buf, t.buf)
		t.buf = buf
	}
	t.buf = append(t.buf, p...)
}

// freeze stops accepting writes and returns the retained tail in order.
func (t *tailWriter) freeze() []byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.frozen = true
	if len(t.buf) < t.max || t.start == 0 {
		out := make([]byte, len(t.buf))
		copy(out, t.buf)
		return out
	}
	out := make([]byte, 0, len(t.buf))
	out = append(out, t.buf[t.start:]...)
	out = append(out, t.buf[:t.start]...)
	return out
}

// Truncated reports whether more bytes were written than were retained.
func (t *tailWriter) Truncated() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.total > int64(len(t.buf))
}

// buffered reports the number of bytes currently retained in memory.
func (t *tailWriter) buffered() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.buf)
}

// stepOutput writes one step's output to "<final>.partial" and renames it to
// "<final>" only when the step completed.
//
// With a positive size limit the bytes are kept in a bounded in-memory tail
// and flushed at the end. With a zero limit the bytes stream straight to disk.
// Either way writes after freeze() are discarded, so a command that ignores
// cancellation cannot corrupt or extend a finished artifact.
type stepOutput struct {
	finalPath   string
	partialPath string

	mu       sync.Mutex
	tail     *tailWriter
	file     *os.File
	frozen   bool
	started  bool
	writeErr error
	// head retains a short prefix of the output so failure details can quote
	// the first diagnostic line (e.g. "command not found").
	head []byte
}

// headLimit bounds the retained prefix used for failure details.
const headLimit = 512

func newStepOutput(finalPath string, max int64) *stepOutput {
	o := &stepOutput{
		finalPath:   finalPath,
		partialPath: finalPath + partialSuffix,
	}
	if max > 0 {
		limit := max
		if limit > int64(maxInt) {
			limit = int64(maxInt)
		}
		o.tail = newTailWriter(int(limit))
	}
	return o
}

// maxInt is the largest value representable by int on this platform.
const maxInt = int(^uint(0) >> 1)

// Writer returns the single writer that must be assigned to both stdout and
// stderr. Using one writer (rather than two) makes the Go runtime share a
// single fd, so the two streams keep their original relative order instead of
// being interleaved by two independent copy goroutines.
//
// The .partial file is created as soon as a step starts, which means an
// interrupted export (even a hard kill) never leaves an unmarked artifact.
func (o *stepOutput) Writer() (io.Writer, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.frozen {
		return discardWriter{}, nil
	}
	if !o.started {
		o.started = true
		if err := os.MkdirAll(filepath.Dir(o.partialPath), 0755); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(o.partialPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			return nil, err
		}
		o.file = f
	}
	if o.tail != nil {
		return tailOutputWriter{o: o}, nil
	}
	return fileOutputWriter{o: o}, nil
}

// Write implements io.Writer. Writes after freeze are discarded.
func (o *stepOutput) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.writeLocked(p)
}

// writeLocked is Write with o.mu held.
func (o *stepOutput) writeLocked(p []byte) (int, error) {
	if o.frozen {
		return len(p), nil
	}
	if len(o.head) < headLimit {
		space := headLimit - len(o.head)
		if space > len(p) {
			space = len(p)
		}
		o.head = append(o.head, p[:space]...)
	}
	if o.tail != nil {
		return o.tail.Write(p)
	}
	if o.file == nil {
		return len(p), nil
	}
	n, err := o.file.Write(p)
	if err != nil && o.writeErr == nil {
		o.writeErr = err
	}
	return n, err
}

// FirstLine returns a short, sanitized excerpt of the beginning of the
// output, suitable for a failure detail.
func (o *stepOutput) FirstLine() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return sanitizeShort(string(o.head), 160)
}

// tailOutputWriter routes writes through stepOutput so freeze is honored.
type tailOutputWriter struct{ o *stepOutput }

func (w tailOutputWriter) Write(p []byte) (int, error) {
	w.o.mu.Lock()
	defer w.o.mu.Unlock()
	return w.o.writeLocked(p)
}

// fileOutputWriter routes writes through stepOutput so freeze is honored.
type fileOutputWriter struct{ o *stepOutput }

func (w fileOutputWriter) Write(p []byte) (int, error) {
	w.o.mu.Lock()
	defer w.o.mu.Unlock()
	return w.o.writeLocked(p)
}

// discardWriter absorbs late writes after freeze.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// Finish freezes the output and returns whether it was truncated. When
// complete is true the .partial file is renamed to its final name; otherwise it
// is left in place as an explicit incomplete marker.
func (o *stepOutput) Finish(complete bool) (truncated bool, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.frozen {
		return false, nil
	}
	o.frozen = true

	// Always make the marker visible, even if the step failed before it could
	// start (e.g. role lookup or step setup failed): a missing file is not a
	// substitute for a "this was not collected" record on disk.
	if !o.started {
		o.started = true
		if mkErr := os.MkdirAll(filepath.Dir(o.partialPath), 0755); mkErr != nil {
			return false, mkErr
		}
		f, openErr := os.OpenFile(o.partialPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if openErr != nil {
			return false, openErr
		}
		o.file = f
	}

	// Close the placeholder file before rewriting it from the bounded tail.
	if o.file != nil {
		if err := o.file.Close(); err != nil && o.writeErr == nil {
			o.writeErr = err
		}
		o.file = nil
	}
	if o.tail != nil {
		tail := o.tail.freeze()
		truncated = o.tail.Truncated()
		if err := os.WriteFile(o.partialPath, tail, 0644); err != nil {
			return truncated, err
		}
	}
	if o.writeErr != nil {
		return truncated, o.writeErr
	}
	if complete {
		if rerr := os.Rename(o.partialPath, o.finalPath); rerr != nil {
			return truncated, rerr
		}
	}
	return truncated, nil
}

// CloseForWriteError closes any open file without renaming, used when setup for
// a step (rather than the command itself) failed. It is a no-op if Finish was
// already called.
func (o *stepOutput) CloseForWriteError() {
	_, _ = o.Finish(false)
}
