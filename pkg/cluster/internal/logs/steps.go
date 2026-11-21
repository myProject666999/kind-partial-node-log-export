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

	"sigs.k8s.io/kind/pkg/cluster/constants"
	"sigs.k8s.io/kind/pkg/cluster/nodes"
	"sigs.k8s.io/kind/pkg/exec"
)

// stepKind describes how a step produces its artifact.
type stepKind int

const (
	// stepCommand runs a command and captures its output.
	stepCommand stepKind = iota
	// stepSerialLogs collects the container runtime's logs for the node.
	stepSerialLogs
	// stepDumpDir archives a directory from the node.
	stepDumpDir
)

// step is one declared collection step. The declaration order is the summary
// order and never depends on execution timing.
type step struct {
	name string
	kind stepKind
	// command builds the command for this step. It may rely on ctx because the
	// engine attaches a per-step deadline.
	command func(ctx context.Context, n nodes.Node) exec.Cmd
	// controlPlaneOnly marks a step that only exists on control plane nodes;
	// on any other role it is not-applicable rather than a failure.
	controlPlaneOnly bool
}

// controlPlaneSteps are the static pod components that only exist on control
// plane nodes. They are read with crictl because they are not systemd units.
func controlPlaneSteps() []step {
	names := []string{
		"etcd",
		"kube-apiserver",
		"kube-controller-manager",
		"kube-scheduler",
	}
	steps := make([]step, 0, len(names))
	for _, component := range names {
		c := component
		steps = append(steps, step{
			name:             c + ".log",
			kind:             stepCommand,
			controlPlaneOnly: true,
			// `crictl logs` prints to stdout/stderr; the loop covers restarted
			// containers. `--name` is a regex matched against container names.
			command: func(ctx context.Context, n nodes.Node) exec.Cmd {
				return n.CommandContext(ctx, "sh", "-c",
					fmt.Sprintf(`for id in $(crictl ps -a -q --name %s); do echo "=== container $id ==="; crictl logs "$id"; done`, c),
				)
			},
		})
	}
	return steps
}

// nodeSteps returns the full ordered step list for a node directory.
//
// The provider inspect command is passed in because its file name depends on
// the runtime and it runs on the host rather than inside the node.
func nodeSteps(cmds ProviderLogCommands) []step {
	steps := []step{
		{
			name: "serial.log",
			kind: stepSerialLogs,
		},
		{
			name: "kubernetes-version.txt",
			kind: stepCommand,
			command: func(ctx context.Context, n nodes.Node) exec.Cmd {
				return n.CommandContext(ctx, "cat", "/kind/version")
			},
		},
		{
			name: "system-info.txt",
			kind: stepCommand,
			// Read-only kernel and system information. Hostnames and boot ids
			// are intentionally excluded to keep the output stable.
			command: func(ctx context.Context, n nodes.Node) exec.Cmd {
				return n.CommandContext(ctx, "sh", "-c",
					`uname -a; cat /proc/version; cat /proc/cmdline; cat /etc/os-release 2>/dev/null; cat /proc/meminfo; cat /proc/cpuinfo`)
			},
		},
		{
			name: "journal.log",
			kind: stepCommand,
			command: func(ctx context.Context, n nodes.Node) exec.Cmd {
				return n.CommandContext(ctx, "journalctl", "--no-pager")
			},
		},
		{
			name: "kubelet.log",
			kind: stepCommand,
			command: func(ctx context.Context, n nodes.Node) exec.Cmd {
				return n.CommandContext(ctx, "journalctl", "--no-pager", "-u", "kubelet.service")
			},
		},
		{
			name: "containerd.log",
			kind: stepCommand,
			command: func(ctx context.Context, n nodes.Node) exec.Cmd {
				return n.CommandContext(ctx, "journalctl", "--no-pager", "-u", "containerd.service")
			},
		},
		{
			name: "images.log",
			kind: stepCommand,
			command: func(ctx context.Context, n nodes.Node) exec.Cmd {
				return n.CommandContext(ctx, "crictl", "images")
			},
		},
		{
			name: "var-log/",
			kind: stepDumpDir,
		},
	}
	if cmds.InspectCommand != nil {
		inspectFile := cmds.InspectFile
		if inspectFile == "" {
			inspectFile = "inspect.json"
		}
		inspect := cmds.InspectCommand
		steps = append(steps, step{
			name: inspectFile,
			kind: stepCommand,
			command: func(ctx context.Context, n nodes.Node) exec.Cmd {
				return inspect(ctx, n.String())
			},
		})
	}
	return append(steps, controlPlaneSteps()...)
}

// collectNode collects all steps for one node. Steps run sequentially within a
// node so a single slow step cannot multiply resource use.
func collectNode(dir string, entry *nodeEntry, cmds ProviderLogCommands, opts Options) NodeResult {
	nodeDir := filepath.Join(dir, entry.directory)
	result := NodeResult{
		Name:      SanitizeUTF8(entry.name),
		Directory: entry.directory,
		Role:      SanitizeUTF8(entry.role),
		RoleError: entry.roleErr,
		Status:    StatusSuccess,
	}
	steps := nodeSteps(cmds)
	result.Steps = make([]StepResult, 0, len(steps))

	for _, s := range steps {
		var res StepResult
		switch {
		case s.controlPlaneOnly && entry.roleErr != "":
			// No file is produced: the step could not be evaluated.
			res = StepResult{
				Name:   s.name,
				Status: StatusFailure,
				Reason: reasonRoleUnknown,
				Detail: "control plane applicability cannot be determined without a role",
			}
		case s.controlPlaneOnly && !isControlPlaneRole(entry.role):
			// No file is produced, and this is not a failure.
			res = StepResult{
				Name:   s.name,
				Status: StatusNotApplicable,
				Reason: reasonNotApplicable,
				Detail: fmt.Sprintf("only present on %s nodes", constants.ControlPlaneNodeRoleValue),
			}
		default:
			res = runStep(nodeDir, s, entry.node, opts)
		}
		if res.Status == StatusFailure {
			result.Status = StatusFailure
		}
		result.Steps = append(result.Steps, res)
	}
	return result
}

func runStep(nodeDir string, s step, n nodes.Node, opts Options) StepResult {
	ctx, cancel := context.WithTimeout(context.Background(), opts.StepTimeout)
	defer cancel()

	fileName := s.name

	// Directory dumps get their own artifact lifecycle.
	if s.kind == stepDumpDir {
		return dumpDirStep(ctx, nodeDir, fileName, n, opts)
	}

	finalPath := filepath.Join(nodeDir, fileName)
	out := newStepOutput(finalPath, opts.MaxLogSize)
	w, err := out.Writer()
	if err != nil {
		return unstartedStep(s.name, err)
	}

	var runErr error
	switch s.kind {
	case stepSerialLogs:
		runErr = runSerialLogs(ctx, n, w)
	default:
		cmd := s.command(ctx, n)
		merged := exec.WithoutOutputCapture(w)
		cmd.SetStdout(merged)
		cmd.SetStderr(merged)
		runErr = cmd.Run()
	}

	complete := runErr == nil && ctx.Err() == nil
	truncated, finErr := out.Finish(complete)
	if finErr != nil {
		return StepResult{
			Name: s.name, File: fileName + partialSuffix, Status: StatusFailure,
			Reason: reasonWriteError, Detail: sanitizeShort(finErr.Error(), 300), Incomplete: true,
		}
	}
	if !complete {
		reason, detail := classifyRunError(ctx, runErr)
		// The command's own first line is usually more useful than "exit
		// status 1"; keep it unless this was a timeout.
		if reason != reasonTimeout {
			if first := out.FirstLine(); first != "" {
				detail = first
			}
		}
		return StepResult{
			Name: s.name, File: fileName + partialSuffix, Status: StatusFailure,
			Reason: reason, Detail: detail, Truncated: truncated, Incomplete: true,
		}
	}
	return StepResult{Name: s.name, File: fileName, Status: StatusSuccess, Truncated: truncated}
}

// runSerialLogs collects the container runtime's stored node logs.
func runSerialLogs(ctx context.Context, n nodes.Node, w io.Writer) error {
	// Mark the writer so a provider implementation that hands it to a local
	// command does not get its output duplicated into an unbounded error
	// buffer.
	w = exec.WithoutOutputCapture(w)
	if sc, ok := n.(serialLogsContexter); ok {
		return sc.SerialLogsContext(ctx, w)
	}
	// Fallback: the provider does not support context. The output writer
	// discards writes after the step is frozen, so a late goroutine cannot
	// corrupt a finished artifact.
	type result struct{ err error }
	ch := make(chan result, 1)
	go func() {
		ch <- result{err: n.SerialLogs(w)}
	}()
	select {
	case r := <-ch:
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// dumpDirStep streams /var/log into "<name>.partial" and renames it only on
// success, so a truncated archive can never be mistaken for a complete one.
func dumpDirStep(ctx context.Context, nodeDir, dirName string, n nodes.Node, opts Options) StepResult {
	finalPath := filepath.Join(nodeDir, dirName)
	partialPath := finalPath + partialSuffix
	if err := os.MkdirAll(partialPath, 0755); err != nil {
		return unstartedStep(dirName, err)
	}
	err := DumpDirContext(ctx, opts.Logger, n, "/var/log", partialPath)
	if err != nil || ctx.Err() != nil {
		reason, detail := classifyRunError(ctx, err)
		return StepResult{
			Name: dirName, File: dirName + partialSuffix, Status: StatusFailure,
			Reason: reason, Detail: detail, Incomplete: true,
		}
	}
	truncated := false
	if opts.MaxLogSize > 0 {
		var terr error
		truncated, terr = truncateTreeTails(partialPath, opts.MaxLogSize)
		if terr != nil {
			return StepResult{
				Name: dirName, File: dirName + partialSuffix, Status: StatusFailure,
				Reason: reasonWriteError, Detail: sanitizeShort(terr.Error(), 300), Incomplete: true,
			}
		}
	}
	if err := os.Rename(partialPath, finalPath); err != nil {
		return StepResult{
			Name: dirName, File: dirName + partialSuffix, Status: StatusFailure,
			Reason: reasonWriteError, Detail: sanitizeShort(err.Error(), 300), Incomplete: true,
		}
	}
	return StepResult{Name: dirName, File: dirName + "/", Status: StatusSuccess, Truncated: truncated}
}

// truncateTreeTails rewrites every regular file under root that exceeds max to
// its last max bytes. It reports whether any file was cut.
func truncateTreeTails(root string, max int64) (bool, error) {
	truncated := false
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() <= max {
			return nil
		}
		if err := truncateFileTail(path, info.Size(), max); err != nil {
			return err
		}
		truncated = true
		return nil
	})
	return truncated, err
}

func truncateFileTail(path string, size, max int64) error {
	in, err := os.Open(path)
	if err != nil {
		return err
	}
	defer in.Close()
	if _, err := in.Seek(size-max, io.SeekStart); err != nil {
		return err
	}
	tmp := path + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		_ = os.Remove(tmp)
		return copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return closeErr
	}
	return os.Rename(tmp, path)
}
