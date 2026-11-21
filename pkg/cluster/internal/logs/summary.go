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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"sigs.k8s.io/kind/pkg/exec"
	"sigs.k8s.io/kind/pkg/log"
)

// Status classifies one collection step.
type Status string

const (
	// StatusSuccess means the step ran and its output was written completely.
	StatusSuccess Status = "success"
	// StatusFailure means the step was applicable but could not be completed.
	StatusFailure Status = "failure"
	// StatusNotApplicable means the step does not exist on this node role.
	StatusNotApplicable Status = "not-applicable"
)

// StepResult is the deterministic outcome of one collection step.
//
// The struct contains no timestamps, durations or byte counts so exporting the
// same cluster twice produces byte-identical summaries.
type StepResult struct {
	// Name identifies the step, e.g. "journal.log".
	Name string `json:"name"`
	// File is the path relative to the export root. It points at the
	// ".partial" path when the step did not complete.
	File string `json:"file,omitempty"`
	// Status is one of success, failure, not-applicable.
	Status Status `json:"status"`
	// Reason is a short, stable explanation for failures and for
	// not-applicable steps.
	Reason string `json:"reason,omitempty"`
	// Detail is optional human readable context (sanitized to valid UTF-8).
	Detail string `json:"detail,omitempty"`
	// Truncated records that only the tail of the output was kept.
	Truncated bool `json:"truncated,omitempty"`
	// Incomplete records that the file on disk is a ".partial" artifact.
	Incomplete bool `json:"incomplete,omitempty"`
}

// NodeResult groups the steps of one node.
type NodeResult struct {
	// Name is the original node name, never encoded.
	Name string `json:"name"`
	// Directory is the exported directory name (see EncodeNodeDirName).
	Directory string `json:"directory,omitempty"`
	// Role is the node role if it could be determined.
	Role string `json:"role,omitempty"`
	// RoleError records why the role is unknown, when it is.
	RoleError string `json:"roleError,omitempty"`
	// Status is not-applicable when the node is intentionally not collected
	// (e.g. the external load balancer).
	Status Status `json:"status"`
	// Reason explains a non-collected node.
	Reason string `json:"reason,omitempty"`
	// Steps is in declaration order, never completion order.
	Steps []StepResult `json:"steps,omitempty"`
}

// Summary is the machine readable export manifest (summary.json).
type Summary struct {
	// SchemaVersion allows readers to detect layout changes.
	SchemaVersion int `json:"schemaVersion"`
	// Cluster is the kind cluster name.
	Cluster string `json:"cluster"`
	// KindVersion is the exporting kind binary version.
	KindVersion string `json:"kindVersion,omitempty"`
	// Settings records the configured knobs. They are deterministic (derived
	// from flags, not from the run) and let a snapshot diff explain why two
	// exports differ.
	Settings SummarySettings `json:"settings"`
	// ClusterSteps are top level artifacts such as nodes.txt.
	ClusterSteps []StepResult `json:"clusterSteps,omitempty"`
	// Nodes is sorted by original node name.
	Nodes []NodeResult `json:"nodes"`
}

// SummarySettings captures parameters that change the exported bytes.
type SummarySettings struct {
	Concurrency    int   `json:"concurrency"`
	MaxLogSize     int64 `json:"maxLogSizeBytes"`
	StepTimeoutSec int   `json:"stepTimeoutSeconds"`
}

// schemaVersion is bumped when the on-disk layout or summary shape changes in
// a way that readers must notice.
const schemaVersion = 1

// Options controls collection.
type Options struct {
	// Concurrency bounds how many nodes are collected at once.
	Concurrency int
	// StepTimeout bounds a single step (and role lookup).
	StepTimeout time.Duration
	// MaxLogSize is the maximum number of bytes retained per file. Zero means
	// unlimited (stream directly to disk).
	MaxLogSize int64
	// Logger receives warnings about non-fatal problems.
	Logger log.Logger
	// KindVersion is written to the summary.
	KindVersion string
}

// DefaultOptions returns the defaults used by `kind export logs`.
func DefaultOptions() Options {
	return Options{
		Concurrency: 4,
		StepTimeout: 2 * time.Minute,
		MaxLogSize:  10 << 20, // 10 MiB
		Logger:      log.NoopLogger{},
	}
}

// Validate checks that the options are usable.
func (o Options) Validate() error {
	if o.Concurrency < 1 {
		return fmt.Errorf("concurrency must be at least 1, got %d", o.Concurrency)
	}
	if o.StepTimeout <= 0 {
		return fmt.Errorf("step timeout must be positive, got %s", o.StepTimeout)
	}
	if o.MaxLogSize < 0 {
		return fmt.Errorf("max log size must not be negative, got %d", o.MaxLogSize)
	}
	return nil
}

// ProviderLogCommands describes the runtime specific commands and file names
// used at the top level of the export and inside each node directory.
//
// It lives here (rather than in the providers package) so the collection engine
// never has to import the provider interface and its heavy dependencies.
type ProviderLogCommands struct {
	// InfoFile is the top level cluster information file, e.g. docker-info.txt.
	InfoFile string
	// InfoCommand returns a command that prints runtime information.
	InfoCommand func(ctx context.Context) exec.Cmd
	// InspectFile is the per-node inspection file, e.g. inspect.json.
	InspectFile string
	// InspectCommand returns a command that inspects the named node.
	InspectCommand func(ctx context.Context, nodeName string) exec.Cmd
}

// SanitizeUTF8 replaces invalid UTF-8 bytes with the Unicode replacement
// character so summaries always remain valid UTF-8 and thus parseable.
func SanitizeUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			b.WriteRune(utf8.RuneError)
			i++
			continue
		}
		b.WriteString(s[i : i+size])
		i += size
	}
	return b.String()
}

// sanitizeShort trims detail text to a single, bounded line.
func sanitizeShort(s string, max int) string {
	s = SanitizeUTF8(s)
	s = strings.ReplaceAll(s, "\r\n", "\n")
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > max {
		// cut on a rune boundary
		cut := max
		for cut > 0 && !utf8.RuneStart(s[cut]) {
			cut--
		}
		s = s[:cut]
	}
	return s
}

// reservedTopLevelNames are names that may not be used as a node directory.
func reservedTopLevelNames() map[string]bool {
	return map[string]bool{
		"summary.json":     true,
		"kind-version.txt": true,
		"nodes.txt":        true,
		"docker-info.txt":  true,
		"podman-info.txt":  true,
		"nerdctl-info.txt": true,
		"finch-info.txt":   true,
	}
}

// sortNodesByName sorts entries by node name. Entries with equal names are
// ordered by their encoded directory so the result stays deterministic for
// duplicate names.
func sortNodesByName(nodes []nodeEntry) {
	sort.SliceStable(nodes, func(i, j int) bool {
		if nodes[i].name != nodes[j].name {
			return nodes[i].name < nodes[j].name
		}
		return nodes[i].directory < nodes[j].directory
	})
}

// writeSummary atomically writes summary.json.
func writeSummary(dir string, s *Summary) error {
	final := filepath.Join(dir, "summary.json")
	tmp := final + ".tmp"
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
