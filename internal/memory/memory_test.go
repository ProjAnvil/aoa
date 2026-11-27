package memory

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOpenSeedsDefaultFiles(t *testing.T) {
	m, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{InstructionsFile, IndexFile, ReportedFile} {
		body, err := m.Read(name)
		if err != nil || body == "" {
			t.Fatalf("Read(%q) = %q, %v", name, body, err)
		}
	}
	bundle, err := m.ContextBundle()
	if err != nil || !strings.Contains(bundle, "Instructions") {
		t.Fatalf("bundle = %q, %v", bundle, err)
	}
}

func TestSafePathRejectsTraversal(t *testing.T) {
	m, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"../escape.md", "/etc/passwd", "a/../../b", "bad name.md", ".."} {
		if _, err := m.Read(name); err == nil {
			t.Fatalf("Read(%q) must fail", name)
		}
		if err := m.Write(name, "x"); err == nil {
			t.Fatalf("Write(%q) must fail", name)
		}
	}
}

func TestSafePathBlocksSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "link"), 0o755); err != nil {
		t.Fatal(err)
	}
	_ = os.RemoveAll(filepath.Join(root, "link"))
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	m, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Write("link/escape.md", "x"); err == nil {
		t.Fatal("write through a symlink outside the root must fail")
	}
}

func TestAppendAndProgressPath(t *testing.T) {
	m, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Append(ReportedFile, "- first"); err != nil {
		t.Fatal(err)
	}
	if err := m.Append(ReportedFile, "- second"); err != nil {
		t.Fatal(err)
	}
	body, err := m.Read(ReportedFile)
	if err != nil || !strings.Contains(body, "first") || !strings.Contains(body, "second") {
		t.Fatalf("reported = %q, %v", body, err)
	}
	path := ProgressPath("../../etc/passwd")
	if strings.Contains(path, "..") || !strings.HasPrefix(path, ProgressDir) {
		t.Fatalf("progress path = %q", path)
	}
	if err := m.Write(path, "progress"); err != nil {
		t.Fatal(err)
	}
	got, err := m.Read(path)
	if err != nil || got != "progress" {
		t.Fatalf("progress = %q, %v", got, err)
	}
}
