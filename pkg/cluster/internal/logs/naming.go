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
	"fmt"
	"strconv"
	"strings"

	"sigs.k8s.io/kind/pkg/cluster/nodes"
)

// maxNodeDirNameLength bounds the encoded directory name so it stays well
// below common NAME_MAX limits (255) and leaves room for step file names.
const maxNodeDirNameLength = 200

// isSafeNodeDirByte reports whether b may appear verbatim in a node directory
// name. The set is deliberately conservative so that the same name works on
// case-sensitive and case-insensitive filesystems and in shell globs.
func isSafeNodeDirByte(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z':
		return true
	case b >= 'A' && b <= 'Z':
		return true
	case b >= '0' && b <= '9':
		return true
	case b == '.', b == '_', b == '-':
		return true
	}
	return false
}

// EncodeNodeDirName converts a node name into a single, flat, portable
// directory name without losing information.
//
// Names that are already safe and not reserved are returned unchanged. Every
// other byte is percent-encoded as %XX (uppercase hex), with '%' itself always
// being encoded. Because '%' only ever appears as part of a three byte escape
// sequence, DecodeNodeDirName can always recover the original name exactly.
//
// reserved contains lower-case names that may not be used as node directories
// (for example "summary.json" or "kind-version.txt"). A leading '.' and
// Windows device names are treated as reserved as well. Those names are made
// distinct by escaping their first byte.
func EncodeNodeDirName(name string, reserved map[string]bool) (string, error) {
	var b strings.Builder
	b.Grow(len(name))
	for i := 0; i < len(name); i++ {
		if isSafeNodeDirByte(name[i]) {
			b.WriteByte(name[i])
		} else {
			fmt.Fprintf(&b, "%%%02X", name[i])
		}
	}
	enc := b.String()
	if enc == "" {
		// A container name is never empty, but keep the mapping injective.
		enc = "%"
	} else if isSafeNodeDirByte(name[0]) {
		needEscape := name[0] == '.' ||
			reserved[strings.ToLower(enc)] ||
			isWindowsDeviceName(name)
		if needEscape {
			enc = fmt.Sprintf("%%%02X", name[0]) + enc[1:]
		}
	}
	if len(enc) > maxNodeDirNameLength {
		return "", fmt.Errorf("node name %q encodes to a directory name that is too long (%d bytes, limit %d)", name, len(enc), maxNodeDirNameLength)
	}
	return enc, nil
}

// DecodeNodeDirName is the inverse of EncodeNodeDirName. It exists so that the
// encoding can be proven lossless and so that tooling can recover node names
// from a previously exported directory.
func DecodeNodeDirName(enc string) (string, error) {
	if enc == "%" {
		return "", nil
	}
	out := make([]byte, 0, len(enc))
	for i := 0; i < len(enc); i++ {
		if enc[i] != '%' {
			out = append(out, enc[i])
			continue
		}
		if i+2 >= len(enc) {
			return "", fmt.Errorf("truncated escape sequence in %q", enc)
		}
		v, err := strconv.ParseUint(enc[i+1:i+3], 16, 8)
		if err != nil {
			return "", fmt.Errorf("invalid escape sequence %q: %v", enc[i:i+3], err)
		}
		out = append(out, byte(v))
		i += 2
	}
	return string(out), nil
}

// isWindowsDeviceName reports whether name would be a reserved device name on
// Windows (CON, NUL, COM1, ...). Node directories are created on the host, and
// exports are frequently copied between operating systems.
func isWindowsDeviceName(name string) bool {
	base := name
	if i := strings.IndexByte(base, '.'); i >= 0 {
		base = base[:i]
	}
	upper := strings.ToUpper(base)
	switch upper {
	case "CON", "PRN", "AUX", "NUL":
		return true
	}
	if len(upper) == 4 {
		switch upper[:3] {
		case "COM", "LPT":
			if upper[3] >= '1' && upper[3] <= '9' {
				return true
			}
		}
	}
	return false
}

// nodeEntry is one node selected for export.
type nodeEntry struct {
	// name is the original, unmodified node name.
	name string
	// directory is the encoded, flat directory name.
	directory string
	// node is the handle used to run commands.
	node nodes.Node
	// role is filled in after role detection.
	role string
	// roleErr records why the role is unknown.
	roleErr string
}
