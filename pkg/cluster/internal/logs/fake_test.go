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
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"sigs.k8s.io/kind/pkg/cluster/nodes"
	"sigs.k8s.io/kind/pkg/exec"
)

// ---- fake node ------------------------------------------------------------

// fakeBehavior decides what one command does.
type fakeBehavior func(ctx context.Context, stdout, stderr io.Writer) error

// writeBehavior writes to stdout, then returns an optional error.
func writeBehavior(stdout string, err error) fakeBehavior {
	return func(ctx context.Context, out, errOut io.Writer) error {
		if stdout != "" {
			if _, werr := io.WriteString(out, stdout); werr != nil {
				return werr
			}
		}
		return err
	}
}

// blockBehavior blocks until the context is done.
func blockBehavior() fakeBehavior {
	return func(ctx context.Context, out, errOut io.Writer) error {
		<-ctx.Done()
		return ctx.Err()
	}
}

// tarBehavior emits a minimal valid tar archive so the var-log dump step can
// succeed against a fake node.
func tarBehavior(files map[string]string) fakeBehavior {
	return func(ctx context.Context, out, errOut io.Writer) error {
		tw := tar.NewWriter(out)
		for name, content := range files {
			if err := tw.WriteHeader(&tar.Header{
				Name:     name,
				Mode:     0644,
				Size:     int64(len(content)),
				Typeflag: tar.TypeReg,
			}); err != nil {
				return err
			}
			if _, err := tw.Write([]byte(content)); err != nil {
				return err
			}
		}
		return tw.Close()
	}
}

// hugeTarBehavior emits n bytes split across files without building the whole
// payload in memory.
func hugeTarBehavior(prefix string, files, fileSize int) fakeBehavior {
	return func(ctx context.Context, out, errOut io.Writer) error {
		tw := tar.NewWriter(out)
		chunk := bytes.Repeat([]byte{'x'}, 4096)
		for i := 0; i < files; i++ {
			name := fmt.Sprintf("%s-%d.log", prefix, i)
			if err := tw.WriteHeader(&tar.Header{
				Name: name, Mode: 0644, Size: int64(fileSize), Typeflag: tar.TypeReg,
			}); err != nil {
				return err
			}
			remaining := fileSize
			for remaining > 0 {
				n := len(chunk)
				if n > remaining {
					n = remaining
				}
				if _, err := tw.Write(chunk[:n]); err != nil {
					return err
				}
				remaining -= n
			}
		}
		return tw.Close()
	}
}

type fakeNode struct {
	name    string
	role    string
	roleErr error
	// blockRole makes role lookup block until its context is done.
	blockRole bool
	// noRoleContext disables roleContexter to exercise the fallback path.
	noRoleContext bool
	// serial is written by SerialLogsContext.
	serial string
	// serialErr is returned by SerialLogsContext after writing serial.
	serialErr error
	// blockSerial makes SerialLogsContext block until its context is done.
	blockSerial bool
	// defaultBehavior handles commands that have no specific behavior.
	defaultBehavior fakeBehavior
	// overrides maps "command arg1 arg2" to a behavior.
	overrides map[string]fakeBehavior

	mu    sync.Mutex
	calls []string
}

var _ nodes.Node = &fakeNode{}
var _ roleContexter = &fakeNode{}
var _ serialLogsContexter = &fakeNode{}

func newFakeNode(name, role string) *fakeNode {
	n := &fakeNode{
		name:            name,
		role:            role,
		defaultBehavior: writeBehavior("fake output\n", nil),
		overrides:       map[string]fakeBehavior{},
	}
	inspectRegistry.register(n)
	return n
}

func (n *fakeNode) String() string { return n.name }

func (n *fakeNode) Role() (string, error) {
	if n.roleErr != nil {
		return "", n.roleErr
	}
	return n.role, nil
}

func (n *fakeNode) RoleContext(ctx context.Context) (string, error) {
	if n.blockRole {
		<-ctx.Done()
		return "", ctx.Err()
	}
	if n.roleErr != nil {
		return "", n.roleErr
	}
	return n.role, nil
}

func (n *fakeNode) IP() (string, string, error) { return "", "", nil }

func (n *fakeNode) SerialLogs(w io.Writer) error {
	return n.SerialLogsContext(context.Background(), w)
}

func (n *fakeNode) SerialLogsContext(ctx context.Context, w io.Writer) error {
	if n.blockSerial {
		<-ctx.Done()
		return ctx.Err()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if n.serial != "" {
		if _, err := io.WriteString(w, n.serial); err != nil {
			return err
		}
		return n.serialErr
	}
	if _, err := io.WriteString(w, "serial\n"); err != nil {
		return err
	}
	return n.serialErr
}

func (n *fakeNode) record(key string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.calls = append(n.calls, key)
}

func (n *fakeNode) CallLog() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]string, len(n.calls))
	copy(out, n.calls)
	return out
}

func (n *fakeNode) behaviorFor(key string) fakeBehavior {
	if b, ok := n.overrides[key]; ok {
		return b
	}
	if n.defaultBehavior != nil {
		return n.defaultBehavior
	}
	return writeBehavior("", nil)
}

// Command implements exec.Cmder.
func (n *fakeNode) Command(command string, args ...string) exec.Cmd {
	return n.CommandContext(context.Background(), command, args...)
}

// CommandContext implements exec.Cmder.
func (n *fakeNode) CommandContext(ctx context.Context, command string, args ...string) exec.Cmd {
	key := strings.Join(append([]string{command}, args...), " ")
	n.record(key)
	if ctx == nil {
		ctx = context.Background()
	}
	return &fakeCmd{node: n, key: key, ctx: ctx}
}

// fakeCmd implements exec.Cmd.
type fakeCmd struct {
	node   *fakeNode
	key    string
	ctx    context.Context
	stdout io.Writer
	stderr io.Writer
	env    []string
	stdin  io.Reader
}

var _ exec.Cmd = &fakeCmd{}

func (c *fakeCmd) SetEnv(env ...string) exec.Cmd  { c.env = env; return c }
func (c *fakeCmd) SetStdin(r io.Reader) exec.Cmd  { c.stdin = r; return c }
func (c *fakeCmd) SetStdout(w io.Writer) exec.Cmd { c.stdout = w; return c }
func (c *fakeCmd) SetStderr(w io.Writer) exec.Cmd { c.stderr = w; return c }

func (c *fakeCmd) Run() error {
	out := c.stdout
	if out == nil {
		out = io.Discard
	}
	errOut := c.stderr
	if errOut == nil {
		errOut = io.Discard
	}
	return c.node.behaviorFor(c.key)(c.ctx, out, errOut)
}

// ---- fake provider commands ----------------------------------------------

func fakeProviderCommands() ProviderLogCommands {
	return ProviderLogCommands{
		InfoFile: "docker-info.txt",
		InfoCommand: func(ctx context.Context) exec.Cmd {
			return &fakeCmd{
				node: &fakeNode{overrides: map[string]fakeBehavior{}},
				key:  "docker info",
				ctx:  ctx,
			}
		},
		InspectFile: "inspect.json",
		InspectCommand: func(ctx context.Context, name string) exec.Cmd {
			// Inspect failures are configured per node via the node's own
			// overrides under the "docker inspect <name>" key.
			return &fakeCmd{
				node: inspectRegistry.nodeFor(name),
				key:  "docker inspect " + name,
				ctx:  ctx,
			}
		},
	}
}

// inspectRegistry lets provider-level commands share per-node overrides.
type fakeInspectRegistry struct {
	mu    sync.Mutex
	nodes map[string]*fakeNode
}

func (r *fakeInspectRegistry) register(n *fakeNode) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.nodes == nil {
		r.nodes = map[string]*fakeNode{}
	}
	r.nodes[n.name] = n
}

func (r *fakeInspectRegistry) nodeFor(name string) *fakeNode {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n, ok := r.nodes[name]; ok {
		return n
	}
	return &fakeNode{overrides: map[string]fakeBehavior{}}
}

var inspectRegistry = &fakeInspectRegistry{}

// ---- tree snapshot -------------------------------------------------------

type treeEntry struct {
	isDir   bool
	content string
}

// snapshot captures the exact directory tree under root. Paths are relative
// and slash separated so comparisons are portable.
func snapshot(t *testing.T, root string) map[string]treeEntry {
	t.Helper()
	out := map[string]treeEntry{}
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		rel = filepath.ToSlash(rel)
		if info.IsDir() {
			out[rel] = treeEntry{isDir: true}
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		out[rel] = treeEntry{content: string(data)}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to snapshot %s: %v", root, err)
	}
	return out
}

func paths(entries map[string]treeEntry) []string {
	out := make([]string, 0, len(entries))
	for p := range entries {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func mustRead(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read %s: %v", path, err)
	}
	return string(data)
}

func stepByName(steps []StepResult, name string) *StepResult {
	for i := range steps {
		if steps[i].Name == name {
			return &steps[i]
		}
	}
	return nil
}

func nodeByName(nodes []NodeResult, name string) *NodeResult {
	for i := range nodes {
		if nodes[i].Name == name {
			return &nodes[i]
		}
	}
	return nil
}

func fmtStep(s StepResult) string {
	return fmt.Sprintf("%s=%s(%s)", s.Name, s.Status, s.Reason)
}
