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

package exec

import (
	"bytes"
	"context"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func writeScript(path, content string) error {
	return os.WriteFile(path, []byte(content), 0755)
}

func timeSleepMillis(ms int) { time.Sleep(time.Duration(ms) * time.Millisecond) }

func timeAfter(d time.Duration) <-chan time.Time { return time.After(d) }

func TestWithoutOutputCapture(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}
	tmp := t.TempDir()
	script := tmp + "/emit.sh"
	if err := writeScript(script, `#!/bin/sh
# emit ~2MB of output across both streams
i=0
while [ $i -lt 2000 ]; do
  printf '0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789'
  printf 'abcdefghij' >&2
  i=$((i+1))
done
`); err != nil {
		t.Fatal(err)
	}

	var sink bytes.Buffer
	cmd := Command(script)
	w := WithoutOutputCapture(&sink)
	cmd.SetStdout(w)
	cmd.SetStderr(w)
	if err := cmd.Run(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sink.Len() < 2000*100 {
		t.Fatalf("sink only received %d bytes", sink.Len())
	}
}

func TestWithoutOutputCaptureKeepsMemoryBounded(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}
	tmp := t.TempDir()
	script := tmp + "/emit.sh"
	if err := writeScript(script, `#!/bin/sh
for i in $(seq 1 4000); do
  printf '0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789'
done
`); err != nil {
		t.Fatal(err)
	}

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	cmd := Command(script)
	w := WithoutOutputCapture(&countingWriter{})
	cmd.SetStdout(w)
	cmd.SetStderr(w)
	if err := cmd.Run(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	runtime.GC()
	runtime.ReadMemStats(&after)
	// 4000 * 100 = ~400KB should not result in hundreds of KB retained in a
	// capture buffer. Allow generous slack for GC noise.
	if grown := int64(after.HeapAlloc) - int64(before.HeapAlloc); grown > 200<<10 {
		t.Errorf("heap grew by %d bytes; output may still be buffered", grown)
	}
}

func TestWithoutOutputCaptureFailureHasNoOutput(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}
	tmp := t.TempDir()
	script := tmp + "/fail.sh"
	if err := writeScript(script, `#!/bin/sh
echo "some output"
exit 3
`); err != nil {
		t.Fatal(err)
	}
	var sink bytes.Buffer
	cmd := Command(script)
	w := WithoutOutputCapture(&sink)
	cmd.SetStdout(w)
	cmd.SetStderr(w)
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected an error")
	}
	runErr := RunErrorForError(err)
	if runErr == nil {
		t.Fatalf("expected a RunError, got %T", err)
	}
	if len(runErr.Output) != 0 {
		t.Errorf("RunError.Output should be empty when capture is disabled, got %q", runErr.Output)
	}
	if !strings.Contains(sink.String(), "some output") {
		t.Errorf("output did not reach the sink: %q", sink.String())
	}
}

type countingWriter struct {
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	c.n += int64(len(p))
	return len(p), nil
}

func TestRunWithStdoutReaderContextCancellation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}
	ctx, cancel := context.WithCancel(context.Background())
	// `exec` replaces the shell with sleep so cancellation kills the process
	// that actually holds the pipe, mirroring how provider CLIs behave.
	cmd := CommandContext(ctx, "sh", "-c", "printf 'hello'; exec sleep 30")
	var got syncBuffer
	done := make(chan error, 1)
	go func() {
		done <- RunWithStdoutReaderContext(ctx, cmd, func(r io.Reader) error {
			_, err := io.Copy(&got, r)
			return err
		})
	}()
	// Cancel shortly after output starts flowing.
	for i := 0; i < 100 && got.Len() == 0; i++ {
		timeSleepMillis(10)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			// cancellation is expected to surface as an error
			t.Logf("RunWithStdoutReaderContext returned: %v", err)
		}
	case <-timeAfter(10 * time.Second):
		t.Fatal("RunWithStdoutReaderContext did not return after cancellation")
	}
	if got.String() != "hello" {
		t.Errorf("reader got %q", got.String())
	}
}

// syncBuffer is a mutex protected bytes.Buffer for tests that poll it while a
// copy goroutine writes.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestBoundedBuffer(t *testing.T) {
	b := &boundedBuffer{max: 5}
	for i := 0; i < 100; i++ {
		if _, err := b.Write([]byte("0123456789")); err != nil {
			t.Fatal(err)
		}
		if b.Len() > 5 {
			t.Fatalf("buffer grew to %d", b.Len())
		}
	}
	if got := b.String(); got != "56789" {
		t.Errorf("boundedBuffer = %q", got)
	}
}

func TestRunWithStdoutReaderContextReportsStderr(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}
	cmd := Command("sh", "-c", "printf 'partial'; echo 'boom: cannot read' >&2; exit 2")
	var got bytes.Buffer
	err := RunWithStdoutReaderContext(context.Background(), cmd, func(r io.Reader) error {
		_, cerr := io.Copy(&got, r)
		return cerr
	})
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "cannot read") {
		t.Errorf("stderr diagnostics lost: %v", err)
	}
	if got.String() != "partial" {
		t.Errorf("stdout archive bytes = %q", got.String())
	}
}
