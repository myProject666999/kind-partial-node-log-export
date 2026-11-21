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

import "sync"

// boundedBuffer retains at most max bytes, discarding the oldest ones. It is
// used for diagnostic stderr snippets so a chatty command cannot grow memory.
type boundedBuffer struct {
	mu   sync.Mutex
	max  int
	data []byte
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.max > 0 {
		b.data = append(b.data, p...)
		if len(b.data) > b.max {
			b.data = b.data[len(b.data)-b.max:]
		}
	}
	return len(p), nil
}

func (b *boundedBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.data)
}

func (b *boundedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.data)
}
