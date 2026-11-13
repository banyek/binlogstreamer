//go:build linux || darwin

package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestStartingBinlog(t *testing.T) {
	dir := t.TempDir()
	matcher := regexp.MustCompile(`^binlog\.[0-9]+$`)
	remote := []binlog{{"binlog.000001", 3}, {"binlog.000002", 3}}
	writeFile(t, dir, remote[1].name, "two")
	local, err := listBinlogs(dir, matcher)
	if err != nil {
		t.Fatal(err)
	}
	if got := startingBinlog(remote, local); got != remote[0].name {
		t.Fatalf("start at %q, want missing file %q", got, remote[0].name)
	}
	writeFile(t, dir, remote[0].name, "one")
	local, err = listBinlogs(dir, matcher)
	if err != nil {
		t.Fatal(err)
	}
	if got := startingBinlog(remote, local); got != remote[1].name {
		t.Fatalf("start at %q, want active file %q", got, remote[1].name)
	}
}

func TestCommandArguments(t *testing.T) {
	cfg := configuration{mysqlbinlog: "/path with spaces/mysqlbinlog", port: 3306,
		host: "localhost", user: "user", password: "$secret; 'literal'"}
	cmd := mysqlbinlogCommand(context.Background(), cfg, "/staging dir", "binlog.000001")
	want := []string{cfg.mysqlbinlog, "--raw", "--read-from-remote-server", "--stop-never",
		"--protocol=TCP", "--host=localhost", "--port=3306", "--user=user",
		"--password=" + cfg.password, "--result-file=/staging dir/", "--", "binlog.000001"}
	if !reflect.DeepEqual(cmd.Args, want) {
		t.Fatalf("arguments = %q, want %q", cmd.Args, want)
	}
}

func TestInterruptedStreamPreservesBackups(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "backup files")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, dir, "binlog.000002", "complete")
	script := filepath.Join(t.TempDir(), "fake mysqlbinlog")
	body := `#!/bin/sh
set -eu
for arg in "$@"; do
	case "$arg" in
		--result-file=*) output=${arg#--result-file=} ;;
	esac
done
printf recovered > "${output}binlog.000001"
printf partial > "${output}binlog.000002"
echo 'connection interrupted' >&2
exit 1
`
	if err := os.WriteFile(script, []byte(body), 0700); err != nil {
		t.Fatal(err)
	}
	cfg := configuration{directory: dir, mysqlbinlog: script, retention: time.Hour}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(filepath.Join(dir, "binlog.000002"), old, old); err != nil {
		t.Fatal(err)
	}
	matcher := regexp.MustCompile(`^binlog\.[0-9]+$`)
	err := streamBinlogs(context.Background(), cfg, matcher, "binlog.000001", nil, func(context.Context) ([]binlog, error) {
		return []binlog{{"binlog.000002", 8}}, nil
	})
	if err == nil || !strings.Contains(err.Error(), "connection interrupted") {
		t.Fatalf("want subprocess failure with stderr, got %v", err)
	}
	staged, err := filepath.Glob(filepath.Join(dir, ".binlogstreamer-*", "binlog.000002"))
	if err != nil || len(staged) != 1 {
		t.Fatalf("want one preserved partial file, got %v, %v", staged, err)
	}
	for name, want := range map[string]string{"binlog.000001": "recovered", "binlog.000002": "complete"} {
		got, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || string(got) != want {
			t.Errorf("%s = %q, %v, want %q", name, got, err, want)
		}
	}
}

func TestRetention(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	old := now.Add(-48 * time.Hour)
	names := []string{"binlog.000001", "binlog.000002", "binlog.000001_incomplete", "old-binX123.notes"}
	for _, name := range names {
		writeFile(t, dir, name, "data")
		if err := os.Chtimes(filepath.Join(dir, name), old, old); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("binlog.000002", filepath.Join(dir, "binlog.000000")); err != nil {
		t.Fatal(err)
	}
	cfg := configuration{directory: dir, retention: 24 * time.Hour}
	if err := cleanupBinlogs(cfg, regexp.MustCompile(`^binlog\.[0-9]+$`), "binlog.000002", now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(dir, names[0])); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired binlog still present or stat failed: %v", err)
	}
	for _, name := range append(names[1:], "binlog.000000") {
		if _, err := os.Lstat(filepath.Join(dir, name)); err != nil {
			t.Errorf("%q should survive retention: %v", name, err)
		}
	}
}

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

type testRows struct {
	columns []string
	values  [][]driver.Value
	end     error
}

func (r *testRows) Columns() []string { return r.columns }
func (r *testRows) Close() error      { return nil }
func (r *testRows) Next(values []driver.Value) error {
	if len(r.values) > 0 {
		copy(values, r.values[0])
		r.values = r.values[1:]
		return nil
	}
	if r.end != nil {
		return r.end
	}
	return io.EOF
}

type testConnector struct{ rows *testRows }

func (c testConnector) Driver() driver.Driver { return nil }
func (c testConnector) Connect(context.Context) (driver.Conn, error) {
	return testConn{c.rows}, nil
}

type testConn struct{ rows *testRows }

func (c testConn) Close() error                        { return nil }
func (c testConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (c testConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c testConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return c.rows, nil
}

func TestReadBinlogs(t *testing.T) {
	two := []string{"Log_name", "File_size"}
	three := []string{"Log_name", "File_size", "Encrypted"}
	for _, tc := range []struct {
		name    string
		columns []string
		values  []driver.Value
		end     error
		wantErr string
	}{
		{"two columns", two, []driver.Value{"binlog.000001", int64(42)}, nil, ""},
		{"three columns", three, []driver.Value{"binlog.000001", int64(42), "No"}, nil, ""},
		{"encrypted", three, []driver.Value{"binlog.000001", int64(554), "Yes"}, nil, ""},
		{"invalid encrypted size", three, []driver.Value{"binlog.000001", int64(42), "Yes"}, nil, "encrypted"},
		{"invalid size", two, []driver.Value{"binlog.000001", "invalid"}, nil, "read binary log row"},
		{"interrupted", two, []driver.Value{"binlog.000001", int64(42)}, errors.New("connection lost"), "connection lost"},
		{"empty", two, nil, nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := &testRows{columns: tc.columns, end: tc.end}
			if tc.values != nil {
				input.values = [][]driver.Value{tc.values}
			}
			db := sql.OpenDB(testConnector{input})
			defer db.Close()
			rows, err := db.Query("SHOW BINARY LOGS")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			logs, err := readBinlogs(rows)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) || len(logs) != 0 {
					t.Fatalf("got %v, %v, want no logs and error containing %q", logs, err, tc.wantErr)
				}
				return
			}
			var want []binlog
			if tc.values != nil {
				want = []binlog{{"binlog.000001", 42}}
			}
			if err != nil || !reflect.DeepEqual(logs, want) {
				t.Fatalf("got %v, %v, want %v", logs, err, want)
			}
		})
	}
}

func TestEmptyInventory(t *testing.T) {
	_, err := binlogMatcher(nil)
	if err == nil || !strings.Contains(err.Error(), "no binary logs") {
		t.Fatalf("want empty inventory error, got %v", err)
	}
}

func TestPublishBinlogs(t *testing.T) {
	dir, staging := t.TempDir(), t.TempDir()
	writeFile(t, dir, "binlog.000001", "old")
	writeFile(t, dir, "binlog.000002", "keep")
	writeFile(t, staging, "binlog.000001", "replacement")
	writeFile(t, staging, "binlog.000002", "same")
	writeFile(t, staging, "binlog.000003", "active")
	matcher := regexp.MustCompile(`^binlog\.[0-9]+$`)
	newest, err := publishBinlogs(staging, dir, matcher)
	if err != nil || newest != "binlog.000003" {
		t.Fatalf("publish returned %q, %v", newest, err)
	}
	for name, want := range map[string]string{"binlog.000001": "replacement", "binlog.000002": "keep"} {
		got, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || string(got) != want {
			t.Errorf("%s = %q, %v, want %q", name, got, err, want)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, newest)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("active log was published or stat failed: %v", err)
	}
}

func TestRecoverStagedBinlogs(t *testing.T) {
	dir := t.TempDir()
	staging, err := os.MkdirTemp(dir, ".binlogstreamer-")
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, staging, "binlog.000001", "complete")
	writeFile(t, staging, "binlog.000002", "partial")
	writeFile(t, staging, "binlog.000003", "bad-data")
	writeFile(t, staging, "binlog.000004", "unknown!")
	writeFile(t, dir, "binlog.000003", "keep")
	validator := filepath.Join(t.TempDir(), "mysqlbinlog")
	if err := os.WriteFile(validator, []byte("#!/bin/sh\nfor path do :; done\ngrep -qx complete \"$path\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	cfg := configuration{directory: dir, mysqlbinlog: validator}
	matcher := regexp.MustCompile(`^binlog\.[0-9]+$`)
	remote := []binlog{{"binlog.000001", 8}, {"binlog.000002", 8}, {"binlog.000003", 8}}
	if err := recoverStagedBinlogs(context.Background(), cfg, matcher, remote); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"binlog.000002", "binlog.000003", "binlog.000004"} {
		if _, err := os.Stat(filepath.Join(staging, name)); err != nil {
			t.Errorf("unverified staged file %q should be preserved: %v", name, err)
		}
	}
	for name, want := range map[string]string{"binlog.000001": "complete", "binlog.000003": "keep"} {
		got, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || string(got) != want {
			t.Errorf("%s = %q, %v, want %q", name, got, err, want)
		}
	}
}
