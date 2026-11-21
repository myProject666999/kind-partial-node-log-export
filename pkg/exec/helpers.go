/*
Copyright 2019 The Kubernetes Authors.

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
	"bufio"
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"sync"

	"al.essio.dev/pkg/shellescape"

	"sigs.k8s.io/kind/pkg/errors"
)

// PrettyCommand takes arguments identical to Cmder.Command,
// it returns a pretty printed command that could be pasted into a shell
func PrettyCommand(name string, args ...string) string {
	var out strings.Builder
	out.WriteString(shellescape.Quote(name))
	for _, arg := range args {
		out.WriteByte(' ')
		out.WriteString(shellescape.Quote(arg))
	}
	return out.String()
}

// RunErrorForError returns a RunError if the error contains a RunError.
// Otherwise it returns nil
func RunErrorForError(err error) *RunError {
	var runError *RunError
	for {
		if rErr, ok := err.(*RunError); ok {
			runError = rErr
		}
		if causerErr, ok := err.(errors.Causer); ok {
			err = causerErr.Cause()
		} else {
			break
		}
	}
	return runError
}

// CombinedOutputLines is like os/exec's cmd.CombinedOutput(),
// but over our Cmd interface, and instead of returning the byte buffer of
// stderr + stdout, it scans these for lines and returns a slice of output lines
func CombinedOutputLines(cmd Cmd) (lines []string, err error) {
	var buff bytes.Buffer
	cmd.SetStdout(&buff)
	cmd.SetStderr(&buff)
	err = cmd.Run()
	scanner := bufio.NewScanner(&buff)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, err
}

// OutputLines is like os/exec's cmd.Output(),
// but over our Cmd interface, and instead of returning the byte buffer of
// stdout, it scans these for lines and returns a slice of output lines
func OutputLines(cmd Cmd) (lines []string, err error) {
	var buff bytes.Buffer
	cmd.SetStdout(&buff)
	err = cmd.Run()
	scanner := bufio.NewScanner(&buff)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, err
}

// Output is like os/exec's cmd.Output, but over our Cmd interface
func Output(cmd Cmd) ([]byte, error) {
	var buff bytes.Buffer
	cmd.SetStdout(&buff)
	err := cmd.Run()
	return buff.Bytes(), err
}

// InheritOutput sets cmd's output to write to the current process's stdout and stderr
func InheritOutput(cmd Cmd) Cmd {
	cmd.SetStderr(os.Stderr)
	cmd.SetStdout(os.Stdout)
	return cmd
}

// RunWithStdoutReader runs cmd with stdout piped to readerFunc
func RunWithStdoutReader(cmd Cmd, readerFunc func(io.Reader) error) error {
	return RunWithStdoutReaderContext(context.Background(), cmd, readerFunc)
}

// RunWithStdoutReaderContext is like RunWithStdoutReader, but honors ctx.
// The command is expected to have been created with CommandContext so that
// cancellation actually releases the underlying process; the pipe writer is
// excluded from error capture so that large streams are not buffered in memory.
func RunWithStdoutReaderContext(ctx context.Context, cmd Cmd, readerFunc func(io.Reader) error) error {
	pr, pw, err := os.Pipe()
	if err != nil {
		return err
	}
	// 'pw' is the same writer for stdout and stderr by convention here; make
	// sure the tar stream is not copied into an unbounded error buffer.
	cmd.SetStdout(WithoutOutputCapture(pw))
	// Keep a bounded snippet of stderr so a failure can explain itself (e.g.
	// "tar: /var/log: Cannot open"); the archive itself is stdout only.
	stderr := &boundedBuffer{max: 4096}
	cmd.SetStderr(stderr)

	errCh := make(chan error, 2)
	var prOnce, pwOnce sync.Once
	closePr := func() { prOnce.Do(func() { _ = pr.Close() }) }
	closePw := func() { pwOnce.Do(func() { _ = pw.Close() }) }
	go func() {
		defer closePr()
		errCh <- readerFunc(pr)
	}()
	go func() {
		defer closePw()
		err := cmd.Run()
		if err != nil && stderr.Len() > 0 {
			err = errors.Errorf("%v: %s", err, strings.TrimSpace(stderr.String()))
		}
		errCh <- err
	}()

	var errs []error
	received := 0
	record := func(err error) {
		received++
		if err != nil {
			errs = append(errs, err)
		}
	}
	for received < 2 {
		select {
		case err := <-errCh:
			record(err)
		case <-ctx.Done():
			// Closing our end unblocks the reader. Closing the write end too
			// makes the local command's output copy fail, which releases it
			// even if a grandchild process inherited the real fd and does not
			// exit on cancellation. Neither close can race with the
			// goroutines' own deferred closes.
			closePr()
			closePw()
			errs = append(errs, ctx.Err())
			for received < 2 {
				record(<-errCh)
			}
		}
	}
	return errors.NewAggregate(errs)
}

// RunWithStdinWriter runs cmd with writerFunc piped to stdin
func RunWithStdinWriter(cmd Cmd, writerFunc func(io.Writer) error) error {
	pr, pw, err := os.Pipe()
	if err != nil {
		return err
	}
	cmd.SetStdin(pr)

	return errors.AggregateConcurrent([]func() error{
		func() error {
			defer pw.Close()
			return writerFunc(pw)
		},
		func() error {
			defer pr.Close()
			return cmd.Run()
		},
	})
}
