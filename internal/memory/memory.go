// Package memory implements the durable memory folder described by the
// always-on agent design: instructions, an index, a report ledger, and one
// progress file per task. State lives on disk, not in the process.
package memory

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	InstructionsFile = "instructions.md"
	IndexFile        = "index.md"
	ReportedFile     = "reported.md"
	ProgressDir      = "progress"
)

// Memory is a directory-backed store. Only facts the agent cannot look up
// elsewhere belong here; everything else stays a pointer.
type Memory struct {
	root string
}

// Open creates the memory layout if it does not exist.
func Open(root string) (*Memory, error) {
	if err := os.MkdirAll(filepath.Join(root, ProgressDir), 0o755); err != nil {
		return nil, fmt.Errorf("memory: create %q: %w", root, err)
	}
	defaults := map[string]string{
		InstructionsFile: "# Instructions\n\nDo not change production configuration without approval.\n",
		IndexFile:        "# Index\n",
		ReportedFile:     "# Reported\n",
	}
	for name, body := range defaults {
		path := filepath.Join(root, name)
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
				return nil, fmt.Errorf("memory: seed %q: %w", name, err)
			}
		}
	}
	return &Memory{root: root}, nil
}

// safePath rejects traversal outside the memory root. It checks the resolved
// absolute path, not only the input string, so symlinks and path cleaning
// cannot escape the root.
func (m *Memory) safePath(name string) (string, error) {
	if name == "" || strings.Contains(name, "..") || strings.HasPrefix(name, "/") {
		return "", fmt.Errorf("memory: unsafe name %q", name)
	}
	for _, r := range name {
		if !(r == '.' || r == '/' || r == '-' || r == '_' || (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
			return "", fmt.Errorf("memory: unsafe name %q", name)
		}
	}
	root, err := filepath.Abs(m.root)
	if err != nil {
		return "", fmt.Errorf("memory: resolve root: %w", err)
	}
	// Compare resolved paths on both sides. On macOS /var is a symlink to
	// /private/var, so an unresolved root would reject every valid file.
	if resolved, err := filepath.EvalSymlinks(root); err == nil {
		root = resolved
	}
	path := filepath.Join(root, filepath.Clean(name))
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("memory: path escapes root %q", name)
	}
	if resolved, err := filepath.EvalSymlinks(filepath.Dir(path)); err == nil {
		relDir, err := filepath.Rel(root, resolved)
		if err != nil || relDir == ".." || strings.HasPrefix(relDir, ".."+string(filepath.Separator)) {
			return "", fmt.Errorf("memory: symlink escapes root %q", name)
		}
	}
	return path, nil
}

// Read loads one memory file. Missing files return an empty string.
func (m *Memory) Read(name string) (string, error) {
	path, err := m.safePath(name)
	if err != nil {
		return "", err
	}
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("memory: read %q: %w", name, err)
	}
	return string(b), nil
}

// Write replaces one memory file.
func (m *Memory) Write(name, content string) error {
	path, err := m.safePath(name)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("memory: mkdir for %q: %w", name, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return fmt.Errorf("memory: write %q: %w", name, err)
	}
	return nil
}

// Append adds one line with a single O_APPEND write. Two concurrent runs
// cannot lose each other's ledger entries.
func (m *Memory) Append(name, line string) error {
	path, err := m.safePath(name)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("memory: append %q: %w", name, err)
	}
	defer f.Close()
	if _, err := f.WriteString(line + "\n"); err != nil {
		return fmt.Errorf("memory: append %q: %w", name, err)
	}
	return nil
}

// ContextBundle is loaded at the start of every run. It carries the standing
// instructions and the small index, not the whole history.
func (m *Memory) ContextBundle() (string, error) {
	var b strings.Builder
	for _, name := range []string{InstructionsFile, IndexFile, ReportedFile} {
		body, err := m.Read(name)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "## %s\n%s\n", name, body)
	}
	return b.String(), nil
}

// ProgressPath returns the memory file name for a task. It keeps only safe
// characters so an event source id cannot influence the file layout.
func ProgressPath(taskID string) string {
	var b strings.Builder
	for _, r := range taskID {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	name := b.String()
	if name == "" {
		name = "unknown"
	}
	if len(name) > 96 {
		name = name[:96]
	}
	return filepath.Join(ProgressDir, name+".md")
}
