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
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"sigs.k8s.io/kind/pkg/cluster/constants"
	"sigs.k8s.io/kind/pkg/cluster/nodes"
)

func testOptions(t *testing.T) Options {
	t.Helper()
	opts := DefaultOptions()
	opts.Concurrency = 4
	opts.StepTimeout = 2 * time.Second
	opts.MaxLogSize = 1 << 20
	opts.KindVersion = "kind v0.0.0-test"
	opts.Logger = nil
	return opts
}

func TestCollectAllSuccess(t *testing.T) {
	dir := t.TempDir()
	cp := newFakeNode("kind-control-plane", constants.ControlPlaneNodeRoleValue)
	cp.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(map[string]string{
		"pods/kube-system_etcd.log": "etcd log\n",
	})
	worker := newFakeNode("kind-worker", constants.WorkerNodeRoleValue)
	worker.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(map[string]string{
		"pods/foo.log": "foo log\n",
	})

	sum, err := Collect("kind", dir, []nodes.Node{cp, worker}, fakeProviderCommands(), testOptions(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Cluster level files.
	for _, f := range []string{"summary.json", "kind-version.txt", "nodes.txt", "docker-info.txt"} {
		if _, statErr := os.Stat(filepath.Join(dir, f)); statErr != nil {
			t.Errorf("missing cluster file %s: %v", f, statErr)
		}
	}

	// Control plane: every step success.
	cpResult := nodeByName(sum.Nodes, "kind-control-plane")
	if cpResult == nil {
		t.Fatal("control plane node missing from summary")
	}
	if cpResult.Status != StatusSuccess {
		t.Errorf("control plane status = %s", cpResult.Status)
	}
	for _, s := range cpResult.Steps {
		if s.Status != StatusSuccess {
			t.Errorf("control plane step %s = %s, want success", fmtStep(s), s.Status)
		}
	}

	// Worker: control plane steps are not-applicable, rest success.
	workerResult := nodeByName(sum.Nodes, "kind-worker")
	if workerResult == nil {
		t.Fatal("worker node missing from summary")
	}
	for _, s := range workerResult.Steps {
		if strings.HasSuffix(s.Name, ".log") && strings.Contains(s.Name, "kube-") || s.Name == "etcd.log" {
			if s.Status != StatusNotApplicable {
				t.Errorf("worker step %s = %s, want not-applicable", s.Name, s.Status)
			}
		}
	}
	if s := stepByName(workerResult.Steps, "journal.log"); s == nil || s.Status != StatusSuccess {
		t.Errorf("worker journal.log missing or not success: %+v", s)
	}

	// No partial artifacts anywhere.
	for path, entry := range snapshot(t, dir) {
		if strings.HasSuffix(path, partialSuffix) {
			t.Errorf("unexpected partial artifact %s (%+v)", path, entry)
		}
	}

	// var-log was extracted as a directory.
	if _, statErr := os.Stat(filepath.Join(dir, "kind-worker", "var-log", "pods", "foo.log")); statErr != nil {
		t.Errorf("expected extracted var-log: %v", statErr)
	}
}

func TestCollectOneStepFails(t *testing.T) {
	dir := t.TempDir()
	cp := newFakeNode("kind-control-plane", constants.ControlPlaneNodeRoleValue)
	cp.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)
	// journalctl fails, everything else succeeds.
	cp.overrides["journalctl --no-pager -u kubelet.service"] = writeBehavior("partial kubelet", fmt.Errorf("exit status 1"))

	sum, err := Collect("kind", dir, []nodes.Node{cp}, fakeProviderCommands(), testOptions(t))
	if err == nil {
		t.Fatal("expected an error for the failed step")
	}
	if !strings.Contains(err.Error(), "kubelet.log") {
		t.Errorf("error does not mention the failing step: %v", err)
	}

	res := nodeByName(sum.Nodes, "kind-control-plane")
	s := stepByName(res.Steps, "kubelet.log")
	if s == nil || s.Status != StatusFailure || !s.Incomplete {
		t.Fatalf("kubelet.log = %+v, want failure+incomplete", s)
	}
	if s.Reason != reasonCommandError {
		t.Errorf("reason = %s", s.Reason)
	}
	// Partial file exists with content, final file does not.
	partial := filepath.Join(dir, "kind-control-plane", "kubelet.log"+partialSuffix)
	if got := mustRead(t, partial); got != "partial kubelet" {
		t.Errorf("partial content = %q", got)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "kind-control-plane", "kubelet.log")); !os.IsNotExist(statErr) {
		t.Errorf("final file should not exist after a failed step (err=%v)", statErr)
	}

	// Other steps unaffected.
	if other := stepByName(res.Steps, "journal.log"); other == nil || other.Status != StatusSuccess {
		t.Errorf("journal.log = %+v, want success", other)
	}
}

func TestCollectOneNodeAllStepsFail(t *testing.T) {
	dir := t.TempDir()
	broken := newFakeNode("kind-broken", constants.WorkerNodeRoleValue)
	broken.defaultBehavior = writeBehavior("", fmt.Errorf("node unreachable"))
	broken.serialErr = fmt.Errorf("node unreachable")
	broken.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = func(ctx context.Context, out, errOut io.Writer) error {
		return fmt.Errorf("node unreachable")
	}
	broken.overrides["docker inspect kind-broken"] = writeBehavior("", fmt.Errorf("unreachable"))

	good := newFakeNode("kind-worker", constants.WorkerNodeRoleValue)
	good.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)

	sum, err := Collect("kind", dir, []nodes.Node{broken, good}, fakeProviderCommands(), testOptions(t))
	if err == nil {
		t.Fatal("expected an aggregate error")
	}
	goodRes := nodeByName(sum.Nodes, "kind-worker")
	for _, s := range goodRes.Steps {
		if s.Status == StatusFailure {
			t.Errorf("healthy node step failed: %s", fmtStep(s))
		}
	}
	brokenRes := nodeByName(sum.Nodes, "kind-broken")
	if brokenRes == nil {
		t.Fatal("broken node missing from summary")
	}
	for _, s := range brokenRes.Steps {
		if s.Status == StatusSuccess {
			t.Errorf("broken node step unexpectedly succeeded: %s", s.Name)
		}
	}
	// Directory still exists so evidence location is predictable.
	if _, statErr := os.Stat(filepath.Join(dir, "kind-broken")); statErr != nil {
		t.Errorf("expected a directory for the failing node: %v", statErr)
	}
}

func TestCollectNodeBlocksTimeout(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(t)
	opts.StepTimeout = 100 * time.Millisecond

	blocked := newFakeNode("kind-blocked", constants.WorkerNodeRoleValue)
	blocked.overrides["journalctl --no-pager"] = blockBehavior()
	blocked.blockSerial = true
	blocked.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = func(ctx context.Context, out, errOut io.Writer) error {
		<-ctx.Done()
		return ctx.Err()
	}
	good := newFakeNode("kind-worker", constants.WorkerNodeRoleValue)
	good.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)

	start := time.Now()
	sum, err := Collect("kind", dir, []nodes.Node{blocked, good}, fakeProviderCommands(), opts)
	if err == nil {
		t.Fatal("expected a timeout error")
	}
	if elapsed := time.Since(start); elapsed > 30*time.Second {
		t.Fatalf("collection took too long: %s", elapsed)
	}

	blockedRes := nodeByName(sum.Nodes, "kind-blocked")
	s := stepByName(blockedRes.Steps, "journal.log")
	if s == nil || s.Status != StatusFailure || s.Reason != reasonTimeout || !s.Incomplete {
		t.Fatalf("journal.log = %+v, want timeout failure", s)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "kind-blocked", "journal.log"+partialSuffix)); statErr != nil {
		t.Errorf("expected partial marker: %v", statErr)
	}
	// The other node is unaffected.
	goodRes := nodeByName(sum.Nodes, "kind-worker")
	if goodRes.Status != StatusSuccess {
		t.Errorf("healthy node status = %s: %v", goodRes.Status, err)
	}
}

func TestCollectRoleLookupTimeout(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(t)
	opts.StepTimeout = 100 * time.Millisecond

	n := newFakeNode("kind-mystery", "")
	n.blockRole = true
	n.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)

	sum, err := Collect("kind", dir, []nodes.Node{n}, fakeProviderCommands(), opts)
	if err == nil {
		t.Fatal("expected an error for the unknown role")
	}
	res := nodeByName(sum.Nodes, "kind-mystery")
	if res.RoleError == "" {
		t.Error("expected RoleError to be recorded")
	}
	// Common steps still run.
	if s := stepByName(res.Steps, "journal.log"); s == nil || s.Status != StatusSuccess {
		t.Errorf("journal.log = %+v, want success despite unknown role", s)
	}
	// Control plane steps fail with role-unknown.
	if s := stepByName(res.Steps, "etcd.log"); s == nil || s.Status != StatusFailure || s.Reason != reasonRoleUnknown {
		t.Errorf("etcd.log = %+v, want role-unknown failure", s)
	}
}

func TestCollectNodeNameSlash(t *testing.T) {
	dir := t.TempDir()
	weird := newFakeNode(`kind-0/control-plane\1`, constants.WorkerNodeRoleValue)
	weird.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)

	sum, err := Collect("kind", dir, []nodes.Node{weird}, fakeProviderCommands(), testOptions(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	res := nodeByName(sum.Nodes, `kind-0/control-plane\1`)
	if res == nil {
		t.Fatal("node missing from summary")
	}
	if !strings.HasPrefix(res.Directory, "kind-0%2Fcontrol-plane%5C1") {
		t.Errorf("directory = %q, want percent encoded", res.Directory)
	}
	// Must be a single flat directory at the top level.
	if _, statErr := os.Stat(filepath.Join(dir, res.Directory)); statErr != nil {
		t.Errorf("expected flat node directory: %v", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "kind-0")); !os.IsNotExist(statErr) {
		t.Error("node name leaked into the directory hierarchy")
	}
}

func TestCollectNodeNameReserved(t *testing.T) {
	dir := t.TempDir()
	weird := newFakeNode("summary.json", constants.WorkerNodeRoleValue)
	weird.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)

	sum, err := Collect("kind", dir, []nodes.Node{weird}, fakeProviderCommands(), testOptions(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	res := nodeByName(sum.Nodes, "summary.json")
	if res.Directory == "summary.json" {
		t.Error("reserved name must be escaped")
	}
	// Both the real summary and the node directory exist.
	if _, statErr := os.Stat(filepath.Join(dir, "summary.json")); statErr != nil {
		t.Errorf("summary.json missing: %v", statErr)
	}
	var decoded Summary
	if err := json.Unmarshal([]byte(mustRead(t, filepath.Join(dir, "summary.json"))), &decoded); err != nil {
		t.Fatalf("summary.json is not valid JSON: %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, res.Directory)); statErr != nil {
		t.Errorf("node directory missing: %v", statErr)
	}
}

func TestCollectNonEmptyDirRefused(t *testing.T) {
	dir := t.TempDir()
	existing := filepath.Join(dir, "precious.txt")
	if err := os.WriteFile(existing, []byte("do not touch"), 0644); err != nil {
		t.Fatal(err)
	}
	before := snapshot(t, dir)

	n := newFakeNode("kind-worker", constants.WorkerNodeRoleValue)
	_, err := Collect("kind", dir, []nodes.Node{n}, fakeProviderCommands(), testOptions(t))
	if err == nil {
		t.Fatal("expected refusal for a non-empty directory")
	}
	if !strings.Contains(err.Error(), "not empty") {
		t.Errorf("unexpected error: %v", err)
	}
	after := snapshot(t, dir)
	if len(before) != len(after) {
		t.Errorf("tree changed: before=%v after=%v", paths(before), paths(after))
	}
	for p, e := range before {
		if after[p] != e {
			t.Errorf("entry %s changed", p)
		}
	}
	if got := mustRead(t, existing); got != "do not touch" {
		t.Errorf("existing file modified: %q", got)
	}
}

func TestCollectEmptyDirAllowed(t *testing.T) {
	dir := t.TempDir() // exists and is empty
	n := newFakeNode("kind-worker", constants.WorkerNodeRoleValue)
	n.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)
	if _, err := Collect("kind", dir, []nodes.Node{n}, fakeProviderCommands(), testOptions(t)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "summary.json")); err != nil {
		t.Errorf("summary.json missing: %v", err)
	}
}

func TestCollectSummaryDeterministic(t *testing.T) {
	run := func() []byte {
		dir := t.TempDir()
		a := newFakeNode("kind-worker2", constants.WorkerNodeRoleValue)
		b := newFakeNode("kind-worker", constants.WorkerNodeRoleValue)
		a.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)
		b.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)
		if _, err := Collect("kind", dir, []nodes.Node{a, b}, fakeProviderCommands(), testOptions(t)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		data, err := os.ReadFile(filepath.Join(dir, "summary.json"))
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	first := run()
	second := run()
	if string(first) != string(second) {
		t.Errorf("summary is not deterministic:\n%s\n---\n%s", first, second)
	}
}

func TestCollectSummarySanitizesReasons(t *testing.T) {
	dir := t.TempDir()
	n := newFakeNode("节点-\x01中文", constants.WorkerNodeRoleValue)
	n.overrides["journalctl --no-pager -u kubelet.service"] = writeBehavior("", fmt.Errorf("bad \x00 byte and \xff\xfe invalid utf8"))
	n.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)

	if _, err := Collect("kind", dir, []nodes.Node{n}, fakeProviderCommands(), testOptions(t)); err == nil {
		t.Fatal("expected error")
	}
	raw := mustRead(t, filepath.Join(dir, "summary.json"))
	var decoded Summary
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatalf("summary.json is not parseable: %v", err)
	}
	if !strings.Contains(decoded.Nodes[0].Name, "中文") {
		t.Errorf("Chinese node name lost: %q", decoded.Nodes[0].Name)
	}
	for _, step := range decoded.Nodes[0].Steps {
		if !utf8.ValidString(step.Detail) || !utf8.ValidString(step.Reason) {
			t.Errorf("step strings are not valid UTF-8: %+v", step)
		}
	}
}

func TestCollectWorkerStepsNotApplicable(t *testing.T) {
	dir := t.TempDir()
	worker := newFakeNode("kind-worker", constants.WorkerNodeRoleValue)
	worker.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)

	sum, err := Collect("kind", dir, []nodes.Node{worker}, fakeProviderCommands(), testOptions(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	res := nodeByName(sum.Nodes, "kind-worker")
	for _, name := range []string{"etcd.log", "kube-apiserver.log", "kube-controller-manager.log", "kube-scheduler.log"} {
		s := stepByName(res.Steps, name)
		if s == nil || s.Status != StatusNotApplicable {
			t.Errorf("%s = %+v, want not-applicable", name, s)
		}
	}
	// The crictl command must never run on a worker.
	for _, call := range worker.CallLog() {
		if strings.Contains(call, "crictl ps") {
			t.Errorf("control plane command ran on a worker: %s", call)
		}
	}
}

func TestCollectNotApplicableIsNotAnError(t *testing.T) {
	dir := t.TempDir()
	worker := newFakeNode("kind-worker", constants.WorkerNodeRoleValue)
	worker.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)

	_, err := Collect("kind", dir, []nodes.Node{worker}, fakeProviderCommands(), testOptions(t))
	if err != nil {
		t.Fatalf("not-applicable steps must not fail the export: %v", err)
	}
}

func TestCollectExternalLoadBalancer(t *testing.T) {
	dir := t.TempDir()
	worker := newFakeNode("kind-worker", constants.WorkerNodeRoleValue)
	worker.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)
	lb := newFakeNode("kind-external-load-balancer", constants.ExternalLoadBalancerNodeRoleValue)

	sum, err := Collect("kind", dir, []nodes.Node{worker, lb}, fakeProviderCommands(), testOptions(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	lbRes := nodeByName(sum.Nodes, "kind-external-load-balancer")
	if lbRes == nil || lbRes.Status != StatusNotApplicable {
		t.Fatalf("load balancer = %+v, want not-applicable", lbRes)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "kind-external-load-balancer")); !os.IsNotExist(statErr) {
		t.Error("load balancer should not get a log directory")
	}
	nodesTxt := mustRead(t, filepath.Join(dir, "nodes.txt"))
	if !strings.Contains(nodesTxt, "kind-external-load-balancer") {
		t.Errorf("nodes.txt missing the load balancer:\n%s", nodesTxt)
	}
}

func TestCollectNoNodes(t *testing.T) {
	dir := t.TempDir()
	if _, err := Collect("kind", dir, nil, fakeProviderCommands(), testOptions(t)); err == nil {
		t.Fatal("expected an error when there are no nodes")
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Errorf("refused collection should not write anything: %v", entries)
	}
}

func TestCollectInvalidOptions(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(t)
	opts.Concurrency = 0
	if _, err := Collect("kind", dir, []nodes.Node{newFakeNode("n", "worker")}, fakeProviderCommands(), opts); err == nil {
		t.Error("expected error for zero concurrency")
	}
	opts = testOptions(t)
	opts.StepTimeout = -time.Second
	if _, err := Collect("kind", dir, []nodes.Node{newFakeNode("n", "worker")}, fakeProviderCommands(), opts); err == nil {
		t.Error("expected error for negative timeout")
	}
}

// TestCollectConcurrencyBounded ensures node collection is bounded by
// Options.Concurrency.
func TestCollectConcurrencyBounded(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(t)
	opts.Concurrency = 2
	opts.StepTimeout = 10 * time.Second

	var inFlight int32
	var maxInFlight int32
	gate := make(chan struct{})

	makeNode := func(name string) *fakeNode {
		n := newFakeNode(name, constants.WorkerNodeRoleValue)
		n.overrides["journalctl --no-pager"] = func(ctx context.Context, out, errOut io.Writer) error {
			cur := atomic.AddInt32(&inFlight, 1)
			for {
				old := atomic.LoadInt32(&maxInFlight)
				if cur <= old || atomic.CompareAndSwapInt32(&maxInFlight, old, cur) {
					break
				}
			}
			<-gate
			atomic.AddInt32(&inFlight, -1)
			return nil
		}
		n.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)
		return n
	}
	a, b, c := makeNode("a"), makeNode("b"), makeNode("c")

	done := make(chan struct{})
	go func() {
		_, _ = Collect("kind", dir, []nodes.Node{a, b, c}, fakeProviderCommands(), opts)
		close(done)
	}()
	// Give the collectors time to reach the blocking step, then release them.
	time.Sleep(200 * time.Millisecond)
	close(gate)
	<-done

	if got := atomic.LoadInt32(&maxInFlight); got > int32(opts.Concurrency) {
		t.Errorf("max in-flight nodes = %d, limit %d", got, opts.Concurrency)
	}
	if got := atomic.LoadInt32(&maxInFlight); got < 2 {
		t.Errorf("expected the limit to be reached, max in-flight = %d", got)
	}
}

func TestCollectReadOnlyCommands(t *testing.T) {
	dir := t.TempDir()
	n := newFakeNode("kind-control-plane", constants.ControlPlaneNodeRoleValue)
	n.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)

	if _, err := Collect("kind", dir, []nodes.Node{n}, fakeProviderCommands(), testOptions(t)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, call := range n.CallLog() {
		if !isReadOnlyCommand(call) {
			t.Errorf("non read-only command was run: %s", call)
		}
	}
}

func isReadOnlyCommand(call string) bool {
	readOnlyPrefixes := []string{
		"cat ", "journalctl --no-pager", "crictl images", "crictl ps", "crictl logs",
		"uname ", "sh -c tar", "sh -c uname", "sh -c for id", "docker inspect",
		"docker info",
	}
	for _, p := range readOnlyPrefixes {
		if strings.HasPrefix(call, p) {
			return true
		}
	}
	return false
}

// TestCollectMergesStdoutAndStderr verifies both streams land in one file and
// the relative write order is preserved.
func TestCollectMergesStdoutAndStderr(t *testing.T) {
	dir := t.TempDir()
	n := newFakeNode("kind-worker", constants.WorkerNodeRoleValue)
	n.overrides["journalctl --no-pager -u kubelet.service"] = func(ctx context.Context, out, errOut io.Writer) error {
		_, _ = io.WriteString(out, "out-1\n")
		_, _ = io.WriteString(errOut, "err-1\n")
		_, _ = io.WriteString(out, "out-2\n")
		_, _ = io.WriteString(errOut, "err-2\n")
		return nil
	}
	n.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)

	if _, err := Collect("kind", dir, []nodes.Node{n}, fakeProviderCommands(), testOptions(t)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := mustRead(t, filepath.Join(dir, "kind-worker", "kubelet.log"))
	if got != "out-1\nerr-1\nout-2\nerr-2\n" {
		t.Errorf("merged output = %q", got)
	}
}

// roleOnlyNode exposes only the nodes.Node interface, hiding the optional
// context-aware methods so the engine's fallbacks are exercised.
type roleOnlyNode struct {
	nodes.Node
}

func TestCollectRoleFallbackWithoutContext(t *testing.T) {
	dir := t.TempDir()
	f := newFakeNode("kind-worker", constants.WorkerNodeRoleValue)
	f.overrides["sh -c tar --hard-dereference -C /var/log/ -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)"] = tarBehavior(nil)
	n := &roleOnlyNode{Node: f}

	sum, err := Collect("kind", dir, []nodes.Node{n}, fakeProviderCommands(), testOptions(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	res := nodeByName(sum.Nodes, "kind-worker")
	if res.Role != constants.WorkerNodeRoleValue {
		t.Errorf("role = %q", res.Role)
	}
	if s := stepByName(res.Steps, "serial.log"); s == nil || s.Status != StatusSuccess {
		t.Errorf("serial.log = %+v, want success through the fallback", s)
	}
}
