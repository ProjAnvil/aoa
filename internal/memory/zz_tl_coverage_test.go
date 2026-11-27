package memory

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTLOpenAndReadMissingFile(t *testing.T) {
	root := t.TempDir()
	m, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	body, err := m.Read("missing.md")
	if err != nil || body != "" {
		t.Fatalf("missing file = %q, %v", body, err)
	}
	// Re-open must not overwrite operator edits.
	if err := m.Write(InstructionsFile, "custom instructions"); err != nil {
		t.Fatal(err)
	}
	again, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	got, err := again.Read(InstructionsFile)
	if err != nil || got != "custom instructions" {
		t.Fatalf("re-open = %q, %v", got, err)
	}
}

func TestTLOpenFailsOnFileRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(root, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root); err == nil {
		t.Fatal("Open with a file root must fail")
	}
}

func TestTLWriteFailsOnDirectoryTarget(t *testing.T) {
	m, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Write("progress", "x"); err == nil {
		t.Fatal("Write onto a directory must fail")
	}
}

func TestTLAppendCreatesNestedPath(t *testing.T) {
	m, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Append("progress/nested/note.md", "first"); err != nil {
		t.Fatal(err)
	}
	body, err := m.Read("progress/nested/note.md")
	if err != nil || !strings.Contains(body, "first") {
		t.Fatalf("nested append = %q, %v", body, err)
	}
}

func TestTLContextBundleError(t *testing.T) {
	m, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Replace a default file with a directory so Read fails with a non-ENOENT error.
	if err := os.Remove(filepath.Join(m.root, IndexFile)); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(m.root, IndexFile), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ContextBundle(); err == nil {
		t.Fatal("ContextBundle must surface read errors")
	}
}
