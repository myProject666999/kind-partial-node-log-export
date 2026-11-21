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

import "io"

// OutputCaptureDisabler is implemented by writers that must not be duplicated
// into the error capture buffer of a command.
//
// By default LocalCmd tees all command output into an in-memory buffer so that
// failures can report it. For commands that stream very large output (log
// collection, archives) that buffer is unbounded and defeats any attempt to
// keep memory usage bounded, so callers can opt out.
type OutputCaptureDisabler interface {
	DisableOutputCapture()
}

// noCaptureWriter wraps a writer and marks it as excluding itself from the
// command error capture buffer.
type noCaptureWriter struct {
	w io.Writer
}

var _ OutputCaptureDisabler = &noCaptureWriter{}

func (w *noCaptureWriter) Write(p []byte) (int, error) { return w.w.Write(p) }

// DisableOutputCapture is part of OutputCaptureDisabler
func (w *noCaptureWriter) DisableOutputCapture() {}

// WithoutOutputCapture returns w such that a command using it as both stdout
// and stderr will not have its output copied into RunError.Output.
//
// Assign the returned value to both SetStdout and SetStderr so the runtime
// still shares a single fd (preserving write ordering) while memory use stays
// bounded by whatever w does with the bytes.
func WithoutOutputCapture(w io.Writer) io.Writer {
	if _, ok := w.(OutputCaptureDisabler); ok {
		// already wrapped; keep a single wrapper so interface equality holds
		return w
	}
	return &noCaptureWriter{w: w}
}

// isCaptureDisabled reports whether w opts out of error capture.
func isCaptureDisabled(w io.Writer) bool {
	_, ok := w.(OutputCaptureDisabler)
	return ok
}
