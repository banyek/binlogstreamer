// +build linux darwin
package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/koding/logging" // logging
	"gopkg.in/ini.v1"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"time"
)

type Configuration struct {
	mysqlhost   string // MySQL host to connect, if empty local socket will be used
	mysqluser   string // User to connect MySQL with
	mysqlpass   string // Password for connecting MySQL
	mysqldb     string // Database to connect to
	mysqlport   int    // Port to connect MySQL, if left blank, 3306 will be used as default
	binlogdir   string // Directory to keep binlogs
	mysqlbinlog string // mysqlbinlog binary with full path
	daysKeep    int64  // days to keep binlogs
}

type Binlog struct {
	filename string
	filesize int64
}

var (
	remoteBinlogs  []Binlog
	localBinlogs   []Binlog
	missingBinlogs []Binlog
	logger         = logging.NewLogger("binlogstreamer")
)

func main() {
	configfile := flag.String("cfg", "binlogstreamer.cfg", "Configuration file")
	flag.Parse()
	logger.Notice("Binlogstreamer started")
	logger.Notice("Loading configuration from %s", *configfile)
	config := configure(*configfile)
	remoteBinlogs := getRemoteBinlogs(config)
	localBinlogs := getLocalBinlogs(config)
	missingBinlogs := checkMissingBinlogs(config, localBinlogs, remoteBinlogs)
	go streamBinlogs(config, missingBinlogs)
	cleanupBinlogs(config)
	tick := time.NewTicker(time.Millisecond * 600000).C
	for {
		select {
		case <-tick:
			cleanupBinlogs(config)
		}
	}
}
func cleanupBinlogs(config *Configuration) {
	if config.daysKeep == 0 {
		return
	}
	secondsToKeep := config.daysKeep * 86400
	now := time.Now()
	files, err := ioutil.ReadDir(config.binlogdir)
	if err != nil {
		fmt.Println(err.Error())
	}
	for _, f := range files {
		match, _ := regexp.MatchString("-bin.[0-9]+", f.Name())
		if match {
			fullpath := fmt.Sprint(config.binlogdir, f.Name())
			fileinfo, _ := os.Stat(fullpath)
			fileAgeSeconds := now.Unix() - fileinfo.ModTime().Unix()
			if fileAgeSeconds > secondsToKeep {
				logger.Notice("Removing binglog: %s", f.Name())
				err = os.Remove(fullpath)
				if err != nil {
					fmt.Println(err.Error())
				}

			}

		}
	}
}

func streamBinlogs(config *Configuration, binlogs []Binlog) {
	streamerCmd := fmt.Sprint(
		config.mysqlbinlog,
		" --raw",
		" --read-from-remote-server",
		" --stop-never",
		" --host=", config.mysqlhost,
		" --port=", strconv.Itoa(config.mysqlport),
		" --user=", config.mysqluser,
		" --password=", config.mysqlpass,
		" --result-file=", config.binlogdir, " ",
		binlogs[0].filename,
	)
	logger.Notice("Starting binlog streaming from %s", config.mysqlhost)
	logger.Notice("First binlog: %s", binlogs[0].filename)
	streamer := exec.Command("bash", "-c", streamerCmd)
	_, err := streamer.Output()
	if err != nil {
		panic(err)
	}
	logger.Error("mysqlbinlog utility quit (maybe the remote server is restarted?)")
	os.Exit(1)
}

func checkMissingBinlogs(config *Configuration, local, remote []Binlog) []Binlog {
	var match bool
	var missing []Binlog
	for _, r := range remote {
		match = false
		for _, l := range local {
			if l.filename == r.filename {
				match = true
				if l.filesize != r.filesize {
					logger.Warning("Binlog %s already exists locally, but its size is differs.", l.filename)
					logger.Notice("Renaming to %s_incomplete", l.filename)
					missing = append(missing, r)
					err := os.Rename(fmt.Sprint(config.binlogdir, "/", l.filename), fmt.Sprint(config.binlogdir, "/", l.filename, "_incomplete"))
					if err != nil {
						fmt.Println(err.Error())
					}

				}
			}
		}
		if !match {
			missing = append(missing, r)
		}
	}
	return missing
}

func getLocalBinlogs(config *Configuration) []Binlog {
	logger.Notice("Checking locally existing binlogs")
	files, err := ioutil.ReadDir(config.binlogdir)
	if err != nil {
		fmt.Println(err.Error())
	}
	for _, f := range files {
		match, _ := regexp.MatchString("-bin.[0-9]+", f.Name())
		if match {
			binlog := Binlog{filename: f.Name(), filesize: f.Size()}
			localBinlogs = append(localBinlogs, binlog)
		}
	}
	return localBinlogs
}

func getRemoteBinlogs(config *Configuration) []Binlog {
	var logName string
	var fileSize int64

	logger.Notice("Checking remote binary logs")
	connecturi := fmt.Sprint(config.mysqluser, ":", config.mysqlpass, "@tcp(", config.mysqlhost, ":", config.mysqlport, ")/", config.mysqldb)
	db, err := sql.Open("mysql", connecturi)
	if err != nil {
		err.Error()
	}
	defer db.Close()
	rows, err := db.Query("SHOW BINARY LOGS")
	if err != nil {
		fmt.Println(err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&logName, &fileSize)
		if err != nil {
			fmt.Println(err)
		}
		binlog := Binlog{filename: logName, filesize: fileSize}
		remoteBinlogs = append(remoteBinlogs, binlog)
	}
	return remoteBinlogs
}

func configure(configfile string) *Configuration {
	cfg, err := ini.Load(configfile)
	if err != nil {
		fmt.Println(err.Error())
	}
	portnum, _ := cfg.Section("DEFAULT").Key("mysqlport").Int()
	if portnum == 0 {
		portnum = 3306
	}
	keep, _ := cfg.Section("DEFAULT").Key("keep_days").Int64()
	retcfg := Configuration{
		mysqlhost:   cfg.Section("DEFAULT").Key("mysqlhost").String(),
		mysqluser:   cfg.Section("DEFAULT").Key("mysqluser").String(),
		mysqlpass:   cfg.Section("DEFAULT").Key("mysqlpass").String(),
		mysqldb:     cfg.Section("DEFAULT").Key("mysqldb").String(),
		mysqlport:   portnum,
		binlogdir:   cfg.Section("DEFAULT").Key("binlogdir").String(),
		mysqlbinlog: cfg.Section("DEFAULT").Key("mysqlbinlog").String(),
		daysKeep:    keep,
	}
	logger.Notice("Configuration loaded successfully")
	return &retcfg
}
