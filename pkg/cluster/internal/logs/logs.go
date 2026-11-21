/*
Copyright 2018 The Kubernetes Authors.

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
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"al.essio.dev/pkg/shellescape"

	"sigs.k8s.io/kind/pkg/cluster/nodes"
	"sigs.k8s.io/kind/pkg/errors"
	"sigs.k8s.io/kind/pkg/exec"
	"sigs.k8s.io/kind/pkg/log"
)

// DumpDir dumps the dir nodeDir on the node to the dir hostDir on the host
func DumpDir(logger log.Logger, node nodes.Node, nodeDir, hostDir string) (err error) {
	return DumpDirContext(context.Background(), logger, node, nodeDir, hostDir)
}

// DumpDirContext is like DumpDir, but honors ctx so that a stuck node cannot
// block the export. It never writes an archive to the node or to disk: the tar
// stream is consumed and extracted as it arrives.
func DumpDirContext(ctx context.Context, logger log.Logger, node nodes.Node, nodeDir, hostDir string) (err error) {
	cmd := node.CommandContext(
		ctx,
		"sh", "-c",
		// Tar will exit 1 if a file changed during the archival.
		// We don't care about this, so we're invoking it in a shell
		// And masking out 1 as a return value.
		// Fatal errors will return exit code 2.
		// http://man7.org/linux/man-pages/man1/tar.1.html#RETURN_VALUE
		fmt.Sprintf(
			`tar --hard-dereference -C %s -chf - . || (r=$?; [ $r -eq 1 ] || exit $r)`,
			shellescape.Quote(path.Clean(nodeDir)+"/"),
		),
	)

	return exec.RunWithStdoutReaderContext(ctx, cmd, func(outReader io.Reader) error {
		if err := untar(logger, outReader, hostDir); err != nil {
			return errors.Wrapf(err, "Untarring %q: %v", nodeDir, err)
		}
		return nil
	})
}

// untar reads the tar file from r and writes it into dir.
func untar(logger log.Logger, r io.Reader, dir string) (err error) {
	tr := tar.NewReader(r)
	for {
		f, err := tr.Next()

		switch {
		case err == io.EOF:
			// drain the reader, which may have trailing null bytes
			// we don't want to leave the writer hanging
			_, err := io.Copy(io.Discard, r)
			return err
		case err != nil:
			return errors.Wrapf(err, "tar reading error: %v", err)
		case f == nil:
			continue
		}

		abs, err := safeJoin(dir, f.Name)
		if err != nil {
			return err
		}

		switch f.Typeflag {
		case tar.TypeReg:
			// Some tar producers omit directory entries; create the parent
			// before opening the file.
			if err := os.MkdirAll(filepath.Dir(abs), 0755); err != nil {
				return err
			}
			wf, err := os.OpenFile(abs, os.O_CREATE|os.O_RDWR, os.FileMode(f.Mode))
			if err != nil {
				return err
			}
			n, err := io.Copy(wf, tr)
			if closeErr := wf.Close(); closeErr != nil && err == nil {
				err = closeErr
			}
			if err != nil {
				return errors.Errorf("error writing to %s: %v", abs, err)
			}
			if n != f.Size {
				return errors.Errorf("only wrote %d bytes to %s; expected %d", n, abs, f.Size)
			}
		case tar.TypeDir:
			if _, err := os.Stat(abs); err != nil {
				if err := os.MkdirAll(abs, 0755); err != nil {
					return err
				}
			}
		default:
			logger.Warnf("tar file entry %s contained unsupported file type %v", f.Name, f.Typeflag)
		}
	}
}

// safeJoin joins name onto dir, rejecting entries that would escape dir.
func safeJoin(dir, name string) (string, error) {
	clean := path.Clean("/" + filepath.ToSlash(name))
	if clean == "/" {
		return dir, nil
	}
	rel := strings.TrimPrefix(clean, "/")
	abs := filepath.Join(dir, filepath.FromSlash(rel))
	relCheck, err := filepath.Rel(dir, abs)
	if err != nil || relCheck == ".." || strings.HasPrefix(relCheck, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("tar entry %q escapes destination %q", name, dir)
	}
	return abs, nil
}
