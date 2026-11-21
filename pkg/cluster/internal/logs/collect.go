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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"sigs.k8s.io/kind/pkg/cluster/constants"
	"sigs.k8s.io/kind/pkg/cluster/nodes"
	"sigs.k8s.io/kind/pkg/errors"
	"sigs.k8s.io/kind/pkg/exec"
	"sigs.k8s.io/kind/pkg/log"
)

// Failure reasons. These are stable strings: scripts and tests may match on
// them, so they must not change casually.
const (
	reasonTimeout       = "timeout"
	reasonCommandError  = "command-error"
	reasonRoleUnknown   = "role-unknown"
	reasonNotApplicable = "not-applicable"
	reasonNotKubeNode   = "not-a-kubernetes-node"
	reasonWriteError    = "write-error"
)

// Optional interfaces implemented by provider nodes. The engine prefers these
// because a deadline can then actually release the underlying process. When a
// node does not implement them the engine falls back to the plain method in a
// goroutine and stops waiting at the deadline.
type roleContexter interface {
	RoleContext(context.Context) (string, error)
}

type serialLogsContexter interface {
	SerialLogsContext(context.Context, io.Writer) error
}

// Collect exports logs for allNodes into dir and returns the summary.
//
// Collection is best effort: a node or step that fails never prevents other
// nodes from being collected. The returned error aggregates every failed step
// (not-applicable steps are not failures); summary.json is written before the
// error is returned so callers can always inspect partial results.
func Collect(clusterName, dir string, allNodes []nodes.Node, cmds ProviderLogCommands, opts Options) (*Summary, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if opts.Logger == nil {
		opts.Logger = log.NoopLogger{}
	}
	if len(allNodes) == 0 {
		return nil, fmt.Errorf("no nodes to export logs from")
	}

	if err := prepareOutputDir(dir); err != nil {
		return nil, err
	}

	entries, err := buildEntries(allNodes, cmds)
	if err != nil {
		return nil, err
	}
	if err := checkDuplicateDirs(entries); err != nil {
		return nil, err
	}
	sortNodesByName(entries)

	// Resolve roles in parallel (bounded by Concurrency) so an unresponsive
	// node costs one timeout, not one per node. Role errors are recorded, not
	// fatal.
	roleResults := make([]error, len(entries))
	var roleWG sync.WaitGroup
	roleSem := make(chan struct{}, opts.Concurrency)
	for i := range entries {
		i := i
		roleWG.Add(1)
		roleSem <- struct{}{}
		go func() {
			defer roleWG.Done()
			defer func() { <-roleSem }()
			role, roleErr := resolveRole(entries[i].node, opts.StepTimeout)
			if roleErr != nil {
				entries[i].roleErr = reasonRoleUnknown + ": " + sanitizeShort(roleErr.Error(), 300)
				roleResults[i] = fmt.Errorf("node %q: could not determine role: %v", entries[i].name, roleErr)
				return
			}
			if role == "" {
				entries[i].roleErr = reasonRoleUnknown + ": role label is empty"
				roleResults[i] = fmt.Errorf("node %q: role label is empty", entries[i].name)
				return
			}
			entries[i].role = role
		}()
	}
	roleWG.Wait()
	var roleErrs []error
	for _, e := range roleResults {
		if e != nil {
			roleErrs = append(roleErrs, e)
		}
	}

	sum := &Summary{
		SchemaVersion: schemaVersion,
		Cluster:       SanitizeUTF8(clusterName),
		KindVersion:   SanitizeUTF8(opts.KindVersion),
		Settings: SummarySettings{
			Concurrency:    opts.Concurrency,
			MaxLogSize:     opts.MaxLogSize,
			StepTimeoutSec: int(opts.StepTimeout / time.Second),
		},
	}

	// Cluster level artifacts are collected first so they exist even if every
	// node fails.
	sum.ClusterSteps = collectClusterSteps(dir, entries, cmds, opts)

	// Per-node collection, bounded by a semaphore and written into
	// pre-indexed slots so completion order never affects the summary.
	sum.Nodes = make([]NodeResult, len(entries))
	var wg sync.WaitGroup
	sem := make(chan struct{}, opts.Concurrency)
	for i := range entries {
		i := i
		entry := &entries[i]
		if isNonKubernetesRole(entry.role) {
			sum.Nodes[i] = NodeResult{
				Name:   SanitizeUTF8(entry.name),
				Role:   SanitizeUTF8(entry.role),
				Status: StatusNotApplicable,
				Reason: reasonNotKubeNode,
			}
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			sum.Nodes[i] = collectNode(dir, entry, cmds, opts)
		}()
	}
	wg.Wait()

	// The summary is written last and atomically.
	if err := writeSummary(dir, sum); err != nil {
		return sum, errors.Wrap(err, "failed to write summary.json")
	}

	// Aggregate failures in deterministic order.
	var errs []error
	for _, step := range sum.ClusterSteps {
		if step.Status == StatusFailure {
			errs = append(errs, fmt.Errorf("cluster step %q: %s", step.Name, reasonsOf(step)))
		}
	}
	for _, n := range sum.Nodes {
		for _, step := range n.Steps {
			if step.Status == StatusFailure {
				errs = append(errs, fmt.Errorf("node %q step %q: %s", n.Name, step.Name, reasonsOf(step)))
			}
		}
	}
	errs = append(errs, roleErrs...)
	if len(errs) > 0 {
		return sum, errors.NewAggregate(errs)
	}
	return sum, nil
}

func reasonsOf(step StepResult) string {
	if step.Detail != "" {
		return step.Reason + " (" + step.Detail + ")"
	}
	return step.Reason
}

// CheckOutputDir validates the target directory without modifying it. The
// policy is to refuse a non-empty existing directory so pre-existing evidence
// is never modified.
func CheckOutputDir(dir string) error {
	info, err := os.Stat(dir)
	switch {
	case os.IsNotExist(err):
		return nil
	case err != nil:
		return errors.Wrap(err, "failed to inspect logs directory")
	case !info.IsDir():
		return fmt.Errorf("logs path %q exists and is not a directory", dir)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return errors.Wrap(err, "failed to read logs directory")
	}
	if len(entries) > 0 {
		return fmt.Errorf("logs directory %q is not empty (%d entries); refusing to overwrite existing logs", dir, len(entries))
	}
	return nil
}

// prepareOutputDir validates the target directory and creates it if missing.
func prepareOutputDir(dir string) error {
	if err := CheckOutputDir(dir); err != nil {
		return err
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if mkErr := os.MkdirAll(dir, os.ModePerm); mkErr != nil {
			return errors.Wrap(mkErr, "failed to create logs directory")
		}
	}
	return nil
}

// buildEntries encodes and validates one entry per unique node.
func buildEntries(allNodes []nodes.Node, cmds ProviderLogCommands) ([]nodeEntry, error) {
	reserved := reservedTopLevelNames()
	// The provider's info file is also a top level artifact.
	if cmds.InfoFile != "" {
		reserved[strings.ToLower(cmds.InfoFile)] = true
	}
	seenNode := map[string]bool{}
	entries := make([]nodeEntry, 0, len(allNodes))
	for _, n := range allNodes {
		name := n.String()
		if seenNode[name] {
			continue
		}
		seenNode[name] = true
		dirName, err := EncodeNodeDirName(name, reserved)
		if err != nil {
			return nil, err
		}
		entries = append(entries, nodeEntry{
			name:      name,
			directory: dirName,
			node:      n,
		})
	}
	return entries, nil
}

// checkDuplicateDirs rejects node names whose encoded directories collide,
// including case-insensitive collisions (exports are often moved between
// macOS, Windows and Linux filesystems).
func checkDuplicateDirs(entries []nodeEntry) error {
	seen := map[string]string{}
	for _, e := range entries {
		key := strings.ToLower(e.directory)
		if other, ok := seen[key]; ok {
			return fmt.Errorf("node names %q and %q map to the same export directory %q", other, e.name, e.directory)
		}
		seen[key] = e.name
	}
	return nil
}

func isNonKubernetesRole(role string) bool {
	return role == constants.ExternalLoadBalancerNodeRoleValue ||
		role == constants.ExternalEtcdNodeRoleValue
}

func isControlPlaneRole(role string) bool {
	return role == constants.ControlPlaneNodeRoleValue
}

// resolveRole prefers RoleContext so the deadline releases the process. The
// fallback keeps the call bounded even for nodes that do not implement it.
func resolveRole(n nodes.Node, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if rc, ok := n.(roleContexter); ok {
		return rc.RoleContext(ctx)
	}
	type result struct {
		role string
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		role, err := n.Role()
		ch <- result{role, err}
	}()
	select {
	case r := <-ch:
		return r.role, r.err
	case <-ctx.Done():
		return "", errors.New(reasonTimeout)
	}
}

// collectClusterSteps returns the top level steps. Each runs with its own
// deadline.
func collectClusterSteps(dir string, entries []nodeEntry, cmds ProviderLogCommands, opts Options) []StepResult {
	steps := []StepResult{
		writeClusterFile(dir, "kind-version.txt", opts.KindVersion),
		// nodes.txt lists every node, including ones not collected, so scripts
		// can correlate directories with roles.
		writeClusterFile(dir, "nodes.txt", nodesFileContents(entries)),
	}
	if cmds.InfoFile != "" && cmds.InfoCommand != nil {
		steps = append(steps, runClusterCommand(dir, cmds.InfoFile, cmds.InfoCommand, opts))
	}
	return steps
}

func nodesFileContents(entries []nodeEntry) string {
	var b strings.Builder
	for _, e := range entries {
		role := e.role
		if role == "" {
			role = "<unknown>"
		}
		// The format is tab separated, one record per line. Escape tabs,
		// newlines and other control bytes in the name so a line always maps
		// to exactly one node.
		fmt.Fprintf(&b, "%s\t%s\n", escapeField(e.name), escapeField(role))
	}
	return b.String()
}

// escapeField makes a value safe for a single tab separated line, without
// losing information: control bytes become Go-style escapes.
func escapeField(s string) string {
	s = SanitizeUTF8(s)
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '\t':
			b.WriteString(`\t`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\\':
			b.WriteString(`\\`)
		default:
			if r < 0x20 || r == 0x7f {
				fmt.Fprintf(&b, `\x%02x`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	return b.String()
}

// writeClusterFile writes a small generated file; it is never truncated and
// never subject to a command timeout.
func writeClusterFile(dir, name, content string) StepResult {
	out := newStepOutput(filepath.Join(dir, name), 0)
	if _, err := out.Writer(); err != nil {
		// Nothing could be created, so do not claim a partial artifact.
		return unstartedStep(name, err)
	}
	if _, err := out.Write([]byte(content)); err != nil {
		out.CloseForWriteError()
		return StepResult{Name: name, File: name + partialSuffix, Status: StatusFailure, Reason: reasonWriteError, Detail: sanitizeShort(err.Error(), 300), Incomplete: true}
	}
	truncated, err := out.Finish(true)
	if err != nil {
		return StepResult{Name: name, File: name + partialSuffix, Status: StatusFailure, Reason: reasonWriteError, Detail: sanitizeShort(err.Error(), 300), Incomplete: true}
	}
	return StepResult{Name: name, File: name, Status: StatusSuccess, Truncated: truncated}
}

func runClusterCommand(dir, fileName string, command func(context.Context) exec.Cmd, opts Options) StepResult {
	ctx, cancel := context.WithTimeout(context.Background(), opts.StepTimeout)
	defer cancel()
	out := newStepOutput(filepath.Join(dir, fileName), opts.MaxLogSize)
	w, err := out.Writer()
	if err != nil {
		return unstartedStep(fileName, err)
	}
	cmd := command(ctx)
	// A single writer for both streams keeps them ordered and (because it
	// implements OutputCaptureDisabler) keeps memory bounded.
	merged := exec.WithoutOutputCapture(w)
	cmd.SetStdout(merged)
	cmd.SetStderr(merged)
	runErr := cmd.Run()
	complete := runErr == nil && ctx.Err() == nil
	truncated, finErr := out.Finish(complete)
	if finErr != nil {
		return StepResult{Name: fileName, File: fileName + partialSuffix, Status: StatusFailure, Reason: reasonWriteError, Detail: sanitizeShort(finErr.Error(), 300), Incomplete: true}
	}
	if !complete {
		reason, detail := classifyRunError(ctx, runErr)
		if reason != reasonTimeout {
			if first := out.FirstLine(); first != "" {
				detail = first
			}
		}
		return StepResult{Name: fileName, File: fileName + partialSuffix, Status: StatusFailure, Reason: reason, Detail: detail, Truncated: truncated, Incomplete: true}
	}
	return StepResult{Name: fileName, File: fileName, Status: StatusSuccess, Truncated: truncated}
}

// unstartedStep describes a step whose artifact could not even be created.
// There is no .partial file on disk in that case, so no incomplete marker.
func unstartedStep(name string, err error) StepResult {
	r := StepResult{Name: name, Status: StatusFailure, Reason: reasonWriteError}
	if err != nil {
		r.Detail = sanitizeShort(err.Error(), 300)
	}
	return r
}

// classifyRunError maps a command failure to a stable reason plus a bounded,
// sanitized detail line.
func classifyRunError(ctx context.Context, err error) (string, string) {
	if ctx.Err() == context.DeadlineExceeded {
		return reasonTimeout, "step exceeded its timeout"
	}
	if err != nil {
		return reasonCommandError, sanitizeShort(err.Error(), 300)
	}
	return reasonCommandError, "step did not complete"
}
