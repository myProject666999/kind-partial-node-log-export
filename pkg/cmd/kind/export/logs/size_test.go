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
	"os"
	"testing"

	"sigs.k8s.io/kind/pkg/cluster"
)

func TestParseMaxLogSize(t *testing.T) {
	cases := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{in: "0", want: 0},
		{in: "unlimited", want: 0},
		{in: "UNLIMITED", want: 0},
		{in: "1234", want: 1234},
		{in: "10MiB", want: 10 << 20},
		{in: "10mib", want: 10 << 20},
		{in: "10Mi", want: 10 << 20},
		{in: "512KiB", want: 512 << 10},
		{in: "1GiB", want: 1 << 30},
		{in: "2B", want: 2},
		{in: " 4MiB ", want: 4 << 20},
		{in: "1.5MiB", want: int64(1.5 * (1 << 20))},
		{in: "-1", wantErr: true},
		{in: "-1MiB", wantErr: true},
		{in: "", wantErr: true},
		{in: "abc", wantErr: true},
		{in: "10XiB", wantErr: true},
		{in: "9EiB", wantErr: true},
	}
	for _, tc := range cases {
		got, err := ParseMaxLogSize(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("%q: expected an error, got %d", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: unexpected error: %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%q: got %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestCheckOutputDir(t *testing.T) {
	dir := t.TempDir()

	// Missing directory is acceptable (it will be created).
	if err := cluster.CheckLogsDir(dir + "/does-not-exist-yet"); err != nil {
		t.Errorf("missing dir: %v", err)
	}
	// Empty existing directory is acceptable.
	if err := cluster.CheckLogsDir(dir); err != nil {
		t.Errorf("empty dir: %v", err)
	}
	// Non-empty directory is refused.
	if err := writeFile(dir+"/x.txt", "x"); err != nil {
		t.Fatal(err)
	}
	if err := cluster.CheckLogsDir(dir); err == nil {
		t.Error("non-empty dir should be refused")
	}
}

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0644)
}
