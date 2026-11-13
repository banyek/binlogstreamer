//go:build linux || darwin

package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"gopkg.in/ini.v1"
)

type configuration struct {
	host, user, password, database string
	port                           int
	directory, mysqlbinlog         string
	retention                      time.Duration
}

type binlog struct {
	name string
	size int64
}

func main() {
	path := flag.String("cfg", "binlogstreamer.cfg", "Configuration file")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *path); err != nil && !errors.Is(err, context.Canceled) {
		log.Print(err)
		os.Exit(1)
	}
}

func run(ctx context.Context, path string) error {
	cfg, err := loadConfig(path)
	if err != nil {
		return err
	}
	directory, err := os.Open(cfg.directory)
	if err != nil {
		return fmt.Errorf("open backup directory: %w", err)
	}
	defer directory.Close()
	info, err := directory.Stat()
	if err != nil {
		return fmt.Errorf("inspect backup directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("backup path %q is not a directory", cfg.directory)
	}
	// Serialize publication and retention for streamers sharing this directory.
	if err := syscall.Flock(int(directory.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("lock backup directory %q: %w", cfg.directory, err)
	}

	remote, err := remoteBinlogs(ctx, cfg)
	if err != nil {
		return err
	}
	matcher, err := binlogMatcher(remote)
	if err != nil {
		return err
	}
	local, err := listBinlogs(cfg.directory, matcher)
	if err != nil {
		return err
	}
	return streamBinlogs(ctx, cfg, matcher, startingBinlog(remote, local))
}

func binlogMatcher(remote []binlog) (*regexp.Regexp, error) {
	if len(remote) == 0 {
		return nil, errors.New("server returned no binary logs")
	}
	last := remote[len(remote)-1].name
	prefix := last[:strings.LastIndexByte(last, '.')]
	matcher := regexp.MustCompile("^" + regexp.QuoteMeta(prefix) + `\.[0-9]+$`)
	for _, entry := range remote {
		if !matcher.MatchString(entry.name) {
			return nil, fmt.Errorf("inconsistent remote binary log basename: %q", entry.name)
		}
	}
	return matcher, nil
}

func loadConfig(path string) (configuration, error) {
	file, err := ini.Load(path)
	if err != nil {
		return configuration{}, fmt.Errorf("load configuration %q: %w", path, err)
	}
	section := file.Section(ini.DefaultSection)
	cfg := configuration{
		host:        section.Key("mysqlhost").String(),
		user:        section.Key("mysqluser").String(),
		password:    section.Key("mysqlpass").String(),
		database:    section.Key("mysqldb").String(),
		directory:   section.Key("binlogdir").String(),
		mysqlbinlog: section.Key("mysqlbinlog").String(),
		port:        3306,
	}
	for _, key := range []string{"mysqluser", "binlogdir", "mysqlbinlog"} {
		if section.Key(key).String() == "" {
			return configuration{}, fmt.Errorf("configuration requires %s", key)
		}
	}
	if cfg.host == "" {
		cfg.host = "localhost"
	}
	if value := section.Key("mysqlport").String(); value != "" {
		port, err := strconv.Atoi(value)
		if err != nil || port < 0 || port > 65535 {
			return configuration{}, errors.New("mysqlport must be 0 or a port between 1 and 65535")
		}
		if port != 0 {
			cfg.port = port
		}
	}
	if value := section.Key("keep_days").String(); value != "" {
		const day = 24 * time.Hour
		days, err := strconv.ParseInt(value, 10, 64)
		if err != nil || days < 0 || days > int64((1<<63-1)/day) {
			return configuration{}, errors.New("keep_days must be a non-negative number of days within duration range")
		}
		cfg.retention = time.Duration(days) * day
	}
	return cfg, nil
}

var binlogName = regexp.MustCompile(`^[^/\\]+\.[0-9]+$`)

func remoteBinlogs(ctx context.Context, cfg configuration) ([]binlog, error) {
	settings := mysql.NewConfig()
	settings.User, settings.Passwd, settings.DBName = cfg.user, cfg.password, cfg.database
	settings.Net, settings.Addr = "tcp", net.JoinHostPort(cfg.host, strconv.Itoa(cfg.port))
	connector, err := mysql.NewConnector(settings)
	if err != nil {
		return nil, fmt.Errorf("configure MySQL connection: %w", err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	rows, err := db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return nil, fmt.Errorf("show binary logs: %w", err)
	}
	defer rows.Close()
	return readBinlogs(rows)
}

func readBinlogs(rows *sql.Rows) ([]binlog, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("read binary log columns: %w", err)
	}
	var entry binlog
	var encrypted sql.NullString
	destinations := make([]any, len(columns))
	foundName, foundSize := false, false
	for i, column := range columns {
		switch strings.ToLower(column) {
		case "log_name":
			destinations[i], foundName = &entry.name, true
		case "file_size":
			destinations[i], foundSize = &entry.size, true
		case "encrypted":
			destinations[i] = &encrypted
		default:
			destinations[i] = new(any)
		}
	}
	if !foundName || !foundSize {
		return nil, errors.New("SHOW BINARY LOGS is missing Log_name or File_size")
	}
	var logs []binlog
	for rows.Next() {
		if err := rows.Scan(destinations...); err != nil {
			return nil, fmt.Errorf("read binary log row: %w", err)
		}
		if !binlogName.MatchString(entry.name) || entry.size < 0 {
			return nil, fmt.Errorf("invalid binary log entry %q with size %d", entry.name, entry.size)
		}
		if encrypted.Valid && !strings.EqualFold(encrypted.String, "No") {
			return nil, fmt.Errorf("cannot compare encrypted binary log %q with its local backup", entry.name)
		}
		logs = append(logs, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read binary log inventory: %w", err)
	}
	return logs, nil
}

func listBinlogs(directory string, matcher *regexp.Regexp) (map[string]os.FileInfo, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("read binary log directory %q: %w", directory, err)
	}
	logs := make(map[string]os.FileInfo)
	for _, entry := range entries {
		if !matcher.MatchString(entry.Name()) || !entry.Type().IsRegular() {
			continue
		}
		info, err := entry.Info()
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("inspect binary log %q: %w", entry.Name(), err)
		}
		if info.Mode().IsRegular() {
			logs[entry.Name()] = info
		}
	}
	return logs, nil
}

func startingBinlog(remote []binlog, local map[string]os.FileInfo) string {
	for _, entry := range remote {
		if file := local[entry.name]; file == nil || file.Size() != entry.size {
			return entry.name
		}
	}
	return remote[len(remote)-1].name
}

func mysqlbinlogCommand(ctx context.Context, cfg configuration, staging, first string) *exec.Cmd {
	return exec.CommandContext(ctx, cfg.mysqlbinlog,
		"--raw", "--read-from-remote-server", "--stop-never",
		"--protocol=TCP", "--host="+cfg.host, "--port="+strconv.Itoa(cfg.port),
		"--user="+cfg.user, "--password="+cfg.password,
		"--result-file="+staging+string(os.PathSeparator), "--", first,
	)
}

func streamBinlogs(ctx context.Context, cfg configuration, matcher *regexp.Regexp, first string) error {
	staging, err := os.MkdirTemp(cfg.directory, ".binlogstreamer-")
	if err != nil {
		return fmt.Errorf("create staging directory: %w", err)
	}
	defer func() {
		if err := os.Remove(staging); err != nil {
			log.Printf("Preserved staged binary logs in %s", staging)
		}
	}()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	command := mysqlbinlogCommand(ctx, cfg, staging, first)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		return fmt.Errorf("start mysqlbinlog: %w", err)
	}
	log.Printf("Streaming binary logs starting at %s", first)
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()

	publishTick := time.NewTicker(time.Second)
	defer publishTick.Stop()
	retentionTick := time.NewTicker(10 * time.Minute)
	defer retentionTick.Stop()
	active := first
	if err := cleanupBinlogs(cfg, matcher, active, time.Now()); err != nil {
		log.Print(err)
	}
	for {
		select {
		case err := <-done:
			if _, publishErr := publishBinlogs(staging, cfg.directory, matcher); publishErr != nil {
				return publishErr
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err == nil {
				return errors.New("mysqlbinlog exited unexpectedly")
			}
			detail := strings.TrimSpace(stderr.String())
			if cfg.password != "" {
				detail = strings.ReplaceAll(detail, cfg.password, "[redacted]")
			}
			if detail != "" {
				return fmt.Errorf("mysqlbinlog failed: %w: %s", err, detail)
			}
			return fmt.Errorf("mysqlbinlog failed: %w", err)
		case <-publishTick.C:
			newest, err := publishBinlogs(staging, cfg.directory, matcher)
			if err != nil {
				cancel()
				<-done
				return err
			}
			if newest != "" && earlierBinlog(active, newest) {
				active = newest
			}
		case now := <-retentionTick.C:
			if err := cleanupBinlogs(cfg, matcher, active, now); err != nil {
				log.Print(err)
			}
		}
	}
}

func binlogSequence(name string) string {
	digits := name[strings.LastIndexByte(name, '.')+1:]
	return strings.TrimLeft(digits, "0")
}

func earlierBinlog(a, b string) bool {
	a, b = binlogSequence(a), binlogSequence(b)
	if len(a) != len(b) {
		return len(a) < len(b)
	}
	return a < b
}

func publishBinlogs(staging, directory string, matcher *regexp.Regexp) (string, error) {
	files, err := listBinlogs(staging, matcher)
	if err != nil {
		return "", err
	}
	var newest string
	for name := range files {
		if newest == "" || earlierBinlog(newest, name) {
			newest = name
		}
	}
	// mysqlbinlog opens the next raw output after closing the previous one.
	// The newest output may still be open or may be partial after an exit.
	for name, file := range files {
		if !earlierBinlog(name, newest) {
			continue
		}
		source := filepath.Join(staging, name)
		target := filepath.Join(directory, name)
		existing, err := os.Lstat(target)
		if err == nil {
			if !existing.Mode().IsRegular() {
				return "", fmt.Errorf("cannot publish binary log over non-regular entry %q", target)
			}
			if existing.Size() == file.Size() {
				if err := os.Remove(source); err != nil {
					return "", fmt.Errorf("discard redundant staged binary log %q: %w", name, err)
				}
				continue
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("inspect existing binary log %q: %w", target, err)
		}
		if err := os.Rename(source, target); err != nil {
			return "", fmt.Errorf("publish binary log %q: %w", name, err)
		}
	}
	return newest, nil
}

func cleanupBinlogs(cfg configuration, matcher *regexp.Regexp, active string, now time.Time) error {
	if cfg.retention == 0 {
		return nil
	}
	files, err := listBinlogs(cfg.directory, matcher)
	if err != nil {
		return err
	}
	for name, file := range files {
		if !earlierBinlog(name, active) || now.Sub(file.ModTime()) <= cfg.retention {
			continue
		}
		if err := os.Remove(filepath.Join(cfg.directory, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove expired binary log %q: %w", name, err)
		}
	}
	return nil
}
