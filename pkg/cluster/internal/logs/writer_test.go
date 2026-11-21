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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/kind/pkg/cluster/constants"
	"sigs.k8s.io/kind/pkg/cluster/nodes"
	"sigs.k8s.io/kind/pkg/exec"
)

func TestTailWriterUnderLimit(t *testing.T) {
	w := newTailWriter(100)
	_, _ = io.WriteString(w, "hello")
	if got := string(w.freeze()); got != "hello" {
		t.Errorf("got %q", got)
	}
	if w.Truncated() {
		t.Error("should not be truncated")
	}
}

func TestTailWriterExactLimit(t *testing.T) {
	w := newTailWriter(5)
	_, _ = io.WriteString(w, "abcde")
	if got := string(w.freeze()); got != "abcde" {
		t.Errorf("got %q", got)
	}
	if w.Truncated() {
		t.Error("exactly at the limit is not truncation")
	}
}

func TestTailWriterOverLimitKeepsExactTail(t *testing.T) {
	w := newTailWriter(5)
	_, _ = io.WriteString(w, "abcdefgh")
	if got := string(w.freeze()); got != "defgh" {
		t.Errorf("got %q, want the last 5 bytes", got)
	}
	if !w.Truncated() {
		t.Error("expected truncation")
	}
}

func TestTailWriterHugeSingleWrite(t *testing.T) {
	w := newTailWriter(16)
	payload := bytes.Repeat([]byte("0123456789"), 1000)
	n, err := w.Write(payload)
	if err != nil || n != len(payload) {
		t.Fatalf("Write returned (%d, %v)", n, err)
	}
	tail := w.freeze()
	if len(tail) != 16 {
		t.Fatalf("retained %d bytes, want 16", len(tail))
	}
	if !bytes.Equal(tail, payload[len(payload)-16:]) {
		t.Errorf("tail mismatch: %q", tail)
	}
}

// TestTailWriterBoundedMemory proves a large stream never buffers more than
// the configured limit.
func TestTailWriterBoundedMemory(t *testing.T) {
	const limit = 64 * 1024
	w := newTailWriter(limit)
	chunk := bytes.Repeat([]byte{'z'}, 8*1024)
	for i := 0; i < 512; i++ { // 4 MiB total
		if _, err := w.Write(chunk); err != nil {
			t.Fatal(err)
		}
		if got := cap(w.buf); got > limit {
			t.Fatalf("buffer capacity grew to %d, limit %d", got, limit)
		}
		if got := w.buffered(); got > limit {
			t.Fatalf("buffer holds %d bytes, limit %d", got, limit)
		}
	}
	if w.total != 4<<20 {
		t.Errorf("total = %d, want %d", w.total, 4<<20)
	}
}

func TestTailWriterFreezeDiscardsLateWrites(t *testing.T) {
	w := newTailWriter(10)
	_, _ = io.WriteString(w, "before")
	_ = w.freeze()
	_, _ = io.WriteString(w, "after")
	if got := string(w.freeze()); got != "before" {
		t.Errorf("late write leaked into the freeze output: %q", got)
	}
}

func TestStepOutputPartialThenRename(t *testing.T) {
	dir := t.TempDir()
	final := filepath.Join(dir, "journal.log")
	out := newStepOutput(final, 0)
	w, err := out.Writer()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(w, "content"); err != nil {
		t.Fatal(err)
	}
	// While running only the .partial path exists.
	if _, statErr := os.Stat(final + partialSuffix); statErr != nil {
		t.Fatalf("expected a partial file while writing: %v", statErr)
	}
	if _, statErr := os.Stat(final); !os.IsNotExist(statErr) {
		t.Errorf("final file should not exist yet: %v", statErr)
	}
	truncated, err := out.Finish(true)
	if err != nil || truncated {
		t.Fatalf("Finish returned (%v, %v)", truncated, err)
	}
	if got := mustRead(t, final); got != "content" {
		t.Errorf("content = %q", got)
	}
	if _, statErr := os.Stat(final + partialSuffix); !os.IsNotExist(statErr) {
		t.Errorf("partial file left behind: %v", statErr)
	}
}

func TestStepOutputFailureLeavesPartial(t *testing.T) {
	dir := t.TempDir()
	final := filepath.Join(dir, "kubelet.log")
	out := newStepOutput(final, 0)
	w, err := out.Writer()
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.WriteString(w, "half a log")
	if _, err := out.Finish(false); err != nil {
		t.Fatal(err)
	}
	if got := mustRead(t, final+partialSuffix); got != "half a log" {
		t.Errorf("partial content = %q", got)
	}
	if _, statErr := os.Stat(final); !os.IsNotExist(statErr) {
		t.Errorf("final file must not exist after failure: %v", statErr)
	}
}

// TestCollectHugeOutputTailOnly streams a large generated output through a
// fake node and asserts the artifact is exactly the tail, bounded in memory.
func TestCollectHugeOutputTailOnly(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(t)
	opts.MaxLogSize = 8 << 10 // 8 KiB

	const chunk = 4 << 10
	const chunks = 256 // 1 MiB
	n := newFakeNode("kind-worker", constants.WorkerNodeRoleValue)
	n.overrides["journalctl --no-pager"] = func(ctx context.Context, out, errOut io.Writer) error {
		// Generate deterministic content with no allocation of the full log.
		for i := 0; i < chunks; i++ {
			var buf [chunk]byte
			for j := range buf {
				buf[j] = byte('a' + (i+j)%26)
			}
			if _, err := out.Write(buf[:]); err != nil {
				return err
			}
		}
		return nil
	}
	n.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)

	sum, err := Collect("kind", dir, []nodes.Node{n}, fakeProviderCommands(), opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	res := nodeByName(sum.Nodes, "kind-worker")
	s := stepByName(res.Steps, "journal.log")
	if s == nil || !s.Truncated {
		t.Fatalf("journal.log = %+v, want truncated", s)
	}
	data, rerr := os.ReadFile(filepath.Join(dir, "kind-worker", "journal.log"))
	if rerr != nil {
		t.Fatal(rerr)
	}
	if len(data) != int(opts.MaxLogSize) {
		t.Fatalf("kept %d bytes, want %d", len(data), opts.MaxLogSize)
	}
	// Compare to the expected tail.
	full := make([]byte, 0, chunk*chunks)
	for i := 0; i < chunks; i++ {
		for j := 0; j < chunk; j++ {
			full = append(full, byte('a'+(i+j)%26))
		}
	}
	want := full[len(full)-int(opts.MaxLogSize):]
	if !bytes.Equal(data, want) {
		t.Error("retained bytes are not the exact tail")
	}
}

func TestCollectUnlimitedLogSize(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(t)
	opts.MaxLogSize = 0

	n := newFakeNode("kind-worker", constants.WorkerNodeRoleValue)
	n.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)

	sum, err := Collect("kind", dir, []nodes.Node{n}, fakeProviderCommands(), opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	res := nodeByName(sum.Nodes, "kind-worker")
	if s := stepByName(res.Steps, "journal.log"); s == nil || s.Truncated {
		t.Errorf("journal.log = %+v, want success without truncation", s)
	}
	if got := mustRead(t, filepath.Join(dir, "kind-worker", "journal.log")); got != "fake output\n" {
		t.Errorf("content = %q", got)
	}
}

// TestCollectDumpTruncatesFiles verifies per-file tail truncation for the
// /var/log directory dump.
func TestCollectDumpTruncatesFiles(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(t)
	opts.MaxLogSize = 16

	big := strings.Repeat("a", 40) + "TAIL-MARKER-0123"
	n := newFakeNode("kind-worker", constants.WorkerNodeRoleValue)
	n.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(map[string]string{
		"pods/big.log":   big,
		"pods/small.log": "small\n",
	})
	sum, err := Collect("kind", dir, []nodes.Node{n}, fakeProviderCommands(), opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	res := nodeByName(sum.Nodes, "kind-worker")
	s := stepByName(res.Steps, "var-log/")
	if s == nil || !s.Truncated || s.Status != StatusSuccess {
		t.Fatalf("var-log step = %+v, want truncated success", s)
	}
	got := mustRead(t, filepath.Join(dir, "kind-worker", "var-log", "pods", "big.log"))
	if len(got) != 16 {
		t.Errorf("big.log = %d bytes, want 16", len(got))
	}
	if !strings.HasSuffix(got, big[len(big)-16:]) {
		t.Errorf("big.log is not the tail: %q", got)
	}
	if got := mustRead(t, filepath.Join(dir, "kind-worker", "var-log", "pods", "small.log")); got != "small\n" {
		t.Errorf("small.log was modified: %q", got)
	}
}

func TestCollectNoTempFilesLeft(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(t)
	opts.MaxLogSize = 8
	n := newFakeNode("kind-worker", constants.WorkerNodeRoleValue)
	n.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(map[string]string{
		"pods/big.log": strings.Repeat("z", 100),
	})
	if _, err := Collect("kind", dir, []nodes.Node{n}, fakeProviderCommands(), opts); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for path := range snapshot(t, dir) {
		if strings.HasSuffix(path, ".tmp") {
			t.Errorf("temporary file left behind: %s", path)
		}
	}
}

// TestCollectTimeoutDoesNotHang relies on a blocking step being released by
// the per-step deadline.
func TestCollectTimeoutDoesNotHang(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(t)
	opts.StepTimeout = 150 * time.Millisecond

	n := newFakeNode("kind-worker", constants.WorkerNodeRoleValue)
	n.blockSerial = true
	n.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)

	start := time.Now()
	sum, err := Collect("kind", dir, []nodes.Node{n}, fakeProviderCommands(), opts)
	if err == nil {
		t.Fatal("expected a timeout error")
	}
	if time.Since(start) > 20*time.Second {
		t.Fatal("collection hung past the deadline")
	}
	res := nodeByName(sum.Nodes, "kind-worker")
	s := stepByName(res.Steps, "serial.log")
	if s == nil || s.Reason != reasonTimeout || !s.Incomplete {
		t.Fatalf("serial.log = %+v, want timeout", s)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "kind-worker", "serial.log"+partialSuffix)); statErr != nil {
		t.Errorf("expected partial serial.log: %v", statErr)
	}
}

// TestCollectGeneratedSummaryShape pins the summary structure so consumers can
// rely on it.
func TestCollectGeneratedSummaryShape(t *testing.T) {
	dir := t.TempDir()
	n := newFakeNode("kind-worker", constants.WorkerNodeRoleValue)
	n.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)
	if _, err := Collect("kind", dir, []nodes.Node{n}, fakeProviderCommands(), testOptions(t)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	raw := mustRead(t, filepath.Join(dir, "summary.json"))
	for _, want := range []string{
		`"schemaVersion": 1`,
		`"cluster": "kind"`,
		`"kindVersion": "kind v0.0.0-test"`,
		`"concurrency": 4`,
		`"maxLogSizeBytes": 1048576`,
		`"stepTimeoutSeconds": 2`,
		`"status": "success"`,
		`"status": "not-applicable"`,
	} {
		if !strings.Contains(raw, want) {
			t.Errorf("summary.json missing %s:\n%s", want, raw)
		}
	}
	// Deterministic means no per-run fields.
	for _, unwanted := range []string{"timestamp", "startedAt", "duration", "finishedAt"} {
		if strings.Contains(raw, unwanted) {
			t.Errorf("summary.json contains non-deterministic field %q", unwanted)
		}
	}
}

func TestCollectStepOrderIsDeclarationOrder(t *testing.T) {
	dir := t.TempDir()
	n := newFakeNode("kind-worker", constants.WorkerNodeRoleValue)
	n.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)
	sum, err := Collect("kind", dir, []nodes.Node{n}, fakeProviderCommands(), testOptions(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var got []string
	for _, s := range nodeByName(sum.Nodes, "kind-worker").Steps {
		got = append(got, s.Name)
	}
	want := []string{
		"serial.log", "kubernetes-version.txt", "system-info.txt", "journal.log",
		"kubelet.log", "containerd.log", "images.log", "var-log/", "inspect.json",
		"etcd.log", "kube-apiserver.log", "kube-controller-manager.log", "kube-scheduler.log",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("step order = %v, want %v", got, want)
	}
}

func TestCollectNamesSortedInSummary(t *testing.T) {
	dir := t.TempDir()
	a := newFakeNode("zzz-node", constants.WorkerNodeRoleValue)
	b := newFakeNode("aaa-node", constants.WorkerNodeRoleValue)
	for _, n := range []*fakeNode{a, b} {
		n.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)
	}
	sum, err := Collect("kind", dir, []nodes.Node{a, b}, fakeProviderCommands(), testOptions(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sum.Nodes[0].Name != "aaa-node" || sum.Nodes[1].Name != "zzz-node" {
		t.Errorf("nodes not sorted: %s, %s", sum.Nodes[0].Name, sum.Nodes[1].Name)
	}
}

func TestCollectErrorMentionsEveryFailure(t *testing.T) {
	dir := t.TempDir()
	n1 := newFakeNode("node-a", constants.WorkerNodeRoleValue)
	n2 := newFakeNode("node-b", constants.WorkerNodeRoleValue)
	for _, n := range []*fakeNode{n1, n2} {
		n.overrides["journalctl --no-pager -u kubelet.service"] = writeBehavior("", fmt.Errorf("boom"))
		n.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)
	}
	_, err := Collect("kind", dir, []nodes.Node{n1, n2}, fakeProviderCommands(), testOptions(t))
	if err == nil {
		t.Fatal("expected an error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "node-a") || !strings.Contains(msg, "node-b") {
		t.Errorf("error does not mention both nodes: %v", err)
	}
}

func TestCollectClusterStepFailure(t *testing.T) {
	dir := t.TempDir()
	cmds := fakeProviderCommands()
	cmds.InfoCommand = func(ctx context.Context) exec.Cmd {
		return &fakeCmd{node: &fakeNode{overrides: map[string]fakeBehavior{"docker info": writeBehavior("", fmt.Errorf("daemon down"))}}, key: "docker info", ctx: ctx}
	}
	n := newFakeNode("kind-worker", constants.WorkerNodeRoleValue)
	n.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)

	sum, err := Collect("kind", dir, []nodes.Node{n}, cmds, testOptions(t))
	if err == nil {
		t.Fatal("expected an error for the failed cluster step")
	}
	s := stepByName(sum.ClusterSteps, "docker-info.txt")
	if s == nil || s.Status != StatusFailure {
		t.Fatalf("docker-info.txt = %+v, want failure", s)
	}
	// Nodes are still collected.
	if nodeByName(sum.Nodes, "kind-worker").Status != StatusSuccess {
		t.Error("node collection should not be affected by a cluster step failure")
	}
}

// TestCollectFullTree compares the complete directory tree for a successful
// export, including every artifact name and whether it is a directory.
func TestCollectFullTree(t *testing.T) {
	dir := t.TempDir()
	n := newFakeNode("kind-worker", constants.WorkerNodeRoleValue)
	n.serial = "serial log line\n"
	n.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(map[string]string{
		"pods/foo.log": "foo\n",
	})
	if _, err := Collect("kind", dir, []nodes.Node{n}, fakeProviderCommands(), testOptions(t)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := snapshot(t, dir)
	want := map[string]treeEntry{
		"summary.json":                       {},
		"kind-version.txt":                   {},
		"nodes.txt":                          {},
		"docker-info.txt":                    {},
		"kind-worker":                        {isDir: true},
		"kind-worker/serial.log":             {content: "serial log line\n"},
		"kind-worker/kubernetes-version.txt": {content: "fake output\n"},
		"kind-worker/system-info.txt":        {content: "fake output\n"},
		"kind-worker/journal.log":            {content: "fake output\n"},
		"kind-worker/kubelet.log":            {content: "fake output\n"},
		"kind-worker/containerd.log":         {content: "fake output\n"},
		"kind-worker/images.log":             {content: "fake output\n"},
		"kind-worker/inspect.json":           {content: "fake output\n"},
		"kind-worker/var-log":                {isDir: true},
		"kind-worker/var-log/pods":           {isDir: true},
		"kind-worker/var-log/pods/foo.log":   {content: "foo\n"},
	}
	if len(got) != len(want) {
		t.Errorf("tree size mismatch:\n got: %v\nwant: %v", paths(got), sortedKeys(want))
	}
	for p, w := range want {
		g, ok := got[p]
		if !ok {
			t.Errorf("missing %s", p)
			continue
		}
		if g.isDir != w.isDir {
			t.Errorf("%s: isDir = %v, want %v", p, g.isDir, w.isDir)
			continue
		}
		if w.content != "" && g.content != w.content {
			t.Errorf("%s: content = %q, want %q", p, g.content, w.content)
		}
	}

	// nodes.txt must be a stable, script-readable listing.
	if got := mustRead(t, filepath.Join(dir, "nodes.txt")); got != "kind-worker\tworker\n" {
		t.Errorf("nodes.txt = %q", got)
	}

	// summary.json parses and marks the worker fully successful.
	var sum Summary
	if err := json.Unmarshal([]byte(mustRead(t, filepath.Join(dir, "summary.json"))), &sum); err != nil {
		t.Fatal(err)
	}
	if len(sum.Nodes) != 1 || sum.Nodes[0].Status != StatusSuccess {
		t.Errorf("summary nodes = %+v", sum.Nodes)
	}
	if len(sum.ClusterSteps) != 3 {
		t.Errorf("cluster steps = %d, want 3", len(sum.ClusterSteps))
	}
}

func sortedKeys(m map[string]treeEntry) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestCollectDetailQuotesOutput verifies a failing command's first output line
// is preserved as the failure detail (missing commands often only print there).
func TestCollectDetailQuotesOutput(t *testing.T) {
	dir := t.TempDir()
	n := newFakeNode("kind-worker", constants.WorkerNodeRoleValue)
	n.overrides["crictl images"] = func(ctx context.Context, out, errOut io.Writer) error {
		_, _ = io.WriteString(errOut, "crictl: command not found\nexit status 127\n")
		return fmt.Errorf("exit status 127")
	}
	n.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)

	sum, err := Collect("kind", dir, []nodes.Node{n}, fakeProviderCommands(), testOptions(t))
	if err == nil {
		t.Fatal("expected an error")
	}
	s := stepByName(nodeByName(sum.Nodes, "kind-worker").Steps, "images.log")
	if s == nil || s.Status != StatusFailure {
		t.Fatalf("images.log = %+v", s)
	}
	if !strings.Contains(s.Detail, "command not found") {
		t.Errorf("detail lost the command output: %q", s.Detail)
	}
	if !strings.Contains(err.Error(), "images.log") {
		t.Errorf("error does not identify the step: %v", err)
	}
	// stdout and stderr end up in the same partial file, in order.
	got := mustRead(t, filepath.Join(dir, "kind-worker", "images.log"+partialSuffix))
	if !strings.Contains(got, "command not found") {
		t.Errorf("partial file lost stderr: %q", got)
	}
}
