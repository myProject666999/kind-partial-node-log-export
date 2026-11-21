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
)

// ParseMaxLogSize parses a human readable size into a byte count.
//
// Accepted forms: "0" or "unlimited" (no limit), a plain byte count ("1234"),
// or a number with a B, Ki, Mi, Gi, Ti binary suffix (case insensitive, with
// or without a trailing "B"). Negative values are rejected.
func ParseMaxLogSize(s string) (int64, error) {
	v := strings.TrimSpace(s)
	if v == "" {
		return 0, fmt.Errorf("empty --max-log-size value")
	}
	lower := strings.ToLower(v)
	if lower == "unlimited" {
		return 0, nil
	}
	if strings.HasPrefix(v, "-") {
		return 0, fmt.Errorf("--max-log-size must not be negative: %q", s)
	}

	// Split the numeric prefix from the suffix.
	i := 0
	for i < len(v) && (v[i] == '.' || (v[i] >= '0' && v[i] <= '9')) {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("invalid --max-log-size %q", s)
	}
	num := v[:i]
	suffix := strings.TrimSpace(v[i:])

	var multiplier int64
	normalizedSuffix := strings.ToLower(suffix)
	normalizedSuffix = strings.TrimSuffix(normalizedSuffix, "b")
	switch normalizedSuffix {
	case "":
		multiplier = 1
	case "k", "ki":
		multiplier = 1 << 10
	case "m", "mi":
		multiplier = 1 << 20
	case "g", "gi":
		multiplier = 1 << 30
	case "t", "ti":
		multiplier = 1 << 40
	default:
		return 0, fmt.Errorf("invalid size suffix %q in --max-log-size %q (use B, KiB, MiB, GiB, or 0)", suffix, s)
	}

	if strings.Contains(num, ".") {
		f, err := strconv.ParseFloat(num, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid --max-log-size %q: %v", s, err)
		}
		if f < 0 {
			return 0, fmt.Errorf("--max-log-size must not be negative: %q", s)
		}
		return int64(f * float64(multiplier)), nil
	}
	n, err := strconv.ParseInt(num, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid --max-log-size %q: %v", s, err)
	}
	if n < 0 {
		return 0, fmt.Errorf("--max-log-size must not be negative: %q", s)
	}
	if n > (1<<63-1)/multiplier {
		return 0, fmt.Errorf("--max-log-size %q overflows", s)
	}
	return n * multiplier, nil
}
