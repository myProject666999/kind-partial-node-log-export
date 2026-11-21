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
	"strings"
	"testing"
	"unicode/utf8"
)

func TestEncodeNodeDirName(t *testing.T) {
	reserved := reservedTopLevelNames()
	cases := []struct {
		name        string
		wantDirect  string // expected encoded name, empty if only uniqueness matters
		wantEscaped bool
	}{
		{name: "kind-control-plane", wantDirect: "kind-control-plane"},
		{name: "worker-1.example", wantDirect: "worker-1.example"},
		{name: "a_b-c", wantDirect: "a_b-c"},
		{name: "a/b", wantDirect: "a%2Fb", wantEscaped: true},
		{name: `a\b`, wantDirect: `a%5Cb`, wantEscaped: true},
		{name: "a b", wantDirect: "a%20b", wantEscaped: true},
		{name: "50%off", wantDirect: "50%25off", wantEscaped: true},
		{name: "%2F", wantDirect: "%252F", wantEscaped: true},
		{name: "中文节点", wantDirect: "%E4%B8%AD%E6%96%87%E8%8A%82%E7%82%B9", wantEscaped: true},
		{name: "..", wantDirect: "%2E.", wantEscaped: true},
		{name: ".hidden", wantEscaped: true},
		{name: "summary.json", wantDirect: "%73ummary.json", wantEscaped: true},
		{name: "kind-version.txt", wantDirect: "%6Bind-version.txt", wantEscaped: true},
		{name: "nodes.txt", wantDirect: "%6Eodes.txt", wantEscaped: true},
		{name: "docker-info.txt", wantDirect: "%64ocker-info.txt", wantEscaped: true},
		{name: "CON", wantEscaped: true},
		{name: "nul", wantEscaped: true},
		{name: "com1", wantEscaped: true},
		{name: "LPT9.log", wantEscaped: true},
		{name: "tab\there", wantEscaped: true},
	}
	seen := map[string]string{}
	for _, tc := range cases {
		got, err := EncodeNodeDirName(tc.name, reserved)
		if err != nil {
			t.Errorf("%q: unexpected error: %v", tc.name, err)
			continue
		}
		if tc.wantDirect != "" && got != tc.wantDirect {
			t.Errorf("%q: got %q, want %q", tc.name, got, tc.wantDirect)
		}
		if tc.wantEscaped && got == tc.name {
			t.Errorf("%q: expected the name to be escaped, got %q", tc.name, got)
		}
		// Must be losslessly decodable.
		decoded, err := DecodeNodeDirName(got)
		if err != nil {
			t.Errorf("%q: decode error: %v", tc.name, err)
			continue
		}
		if decoded != tc.name {
			t.Errorf("%q: round trip produced %q", tc.name, decoded)
		}
		// Must not collide with any earlier mapping.
		key := strings.ToLower(got)
		if other, ok := seen[key]; ok && got != "" {
			t.Errorf("%q and %q both encode to %q", other, tc.name, got)
		}
		seen[key] = tc.name
	}
}

func TestEncodeNodeDirNameDistinguishesReserved(t *testing.T) {
	reserved := reservedTopLevelNames()
	// A literal "summary.json" and a name that already looks escaped must not
	// map to the same directory.
	a, err := EncodeNodeDirName("summary.json", reserved)
	if err != nil {
		t.Fatal(err)
	}
	b, err := EncodeNodeDirName("%73ummary.json", reserved)
	if err != nil {
		t.Fatal(err)
	}
	if strings.EqualFold(a, b) {
		t.Errorf("collision between %q and %q", a, b)
	}
	if b != "%2573ummary.json" {
		t.Errorf("literal %% should be escaped, got %q", b)
	}
}

func TestEncodeNodeDirNameTooLong(t *testing.T) {
	long := strings.Repeat("a", maxNodeDirNameLength+1)
	if _, err := EncodeNodeDirName(long, nil); err == nil {
		t.Error("expected an error for a too long name")
	}
}

func TestDecodeNodeDirNameErrors(t *testing.T) {
	for _, bad := range []string{"abc%", "abc%4", "abc%ZZ"} {
		if _, err := DecodeNodeDirName(bad); err == nil {
			t.Errorf("expected an error decoding %q", bad)
		}
	}
}

func TestSanitizeUTF8(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"plain", "plain"},
		{"中文", "中文"},
		{"a\xffb", "a�b"},
		{"\x00\x01", "\x00\x01"},
	}
	for _, tc := range cases {
		got := SanitizeUTF8(tc.in)
		if got != tc.want {
			t.Errorf("SanitizeUTF8(%q) = %q, want %q", tc.in, got, tc.want)
		}
		if !utf8.ValidString(got) {
			t.Errorf("SanitizeUTF8(%q) produced invalid UTF-8", tc.in)
		}
	}
}

func TestSanitizeShort(t *testing.T) {
	long := strings.Repeat("x", 500)
	got := sanitizeShort(long, 20)
	if len(got) > 20 {
		t.Errorf("not truncated: %d bytes", len(got))
	}
	multi := "line one\nline two"
	if got := sanitizeShort(multi, 100); got != "line one" {
		t.Errorf("multi-line not reduced to first line: %q", got)
	}
	// Must not split a multi-byte rune.
	chinese := strings.Repeat("中", 20)
	got = sanitizeShort(chinese, 19)
	if !utf8.ValidString(got) {
		t.Errorf("split rune: %q", got)
	}
}

func TestEscapeField(t *testing.T) {
	cases := []struct{ in, want string }{
		{"plain", "plain"},
		{"a\tb", `a\tb`},
		{"a\nb", `a\nb`},
		{"a\rb", `a\rb`},
		{`a\b`, `a\\b`},
		{"a\x01b", `a\x01b`},
		{"中文", "中文"},
	}
	for _, tc := range cases {
		if got := escapeField(tc.in); got != tc.want {
			t.Errorf("escapeField(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestCheckDuplicateDirs(t *testing.T) {
	if err := checkDuplicateDirs([]nodeEntry{{name: "A", directory: "dup"}, {name: "a", directory: "DUP"}}); err == nil {
		t.Error("expected a case-insensitive collision error")
	}
	if err := checkDuplicateDirs([]nodeEntry{{name: "a", directory: "a"}, {name: "b", directory: "b"}}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}
