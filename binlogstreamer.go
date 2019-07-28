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

type Streamer struct {
	streamname  string // name of the streamer
	mysqlhost   string // MySQL host to connect, if empty local socket will be used
	mysqluser   string // User to connect MySQL with
	mysqlpass   string // Password for connecting MySQL
	mysqldb     string // Database to connect to
	mysqlport   int    // Port to connect MySQL, if left blank, 3306 will be used as default
	binlogdir   string // Directory to keep binlogs
	mysqlbinlog string // mysqlbinlog binary with full path
	daysKeep    int64  // days to keep binlogs
    remoteBinlogs []Binlog
    localBinlogs []Binlog
    missingBinlogs []Binlog
}

type Binlog struct {
	filename string
	filesize int64
}

var (
	logger         = logging.NewLogger("binlogstreamer")
)

func main() {
	cfg := flag.String("cfg", "binlogstreamer.cfg", "Configuration file")
	flag.Parse()
	logger.Notice("Binlogstreamer started")
	logger.Notice("Loading configuration from %s", *cfg)
	streamers := configure(*cfg)
	for _, streamer := range streamers {
		print(streamer.streamname)
		streamer.remoteBinlogs = getRemoteBinlogs(streamer)
		streamer.localBinlogs = getLocalBinlogs(streamer)
		streamer.missingBinlogs = checkMissingBinlogs(streamer)
		go streamBinlogs(streamer)
		cleanupBinlogs(streamer)
		tick := time.NewTicker(time.Millisecond * 600000).C
		for {
			select {
			case <-tick:
				cleanupBinlogs(streamer)
			}
		}
	}

}
func cleanupBinlogs(config Streamer) {
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

func streamBinlogs(config Streamer) {
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
		config.missingBinlogs[0].filename,
	)
	logger.Notice("Starting binlog streaming from %s", config.mysqlhost)
	logger.Notice("First binlog: %s", config.missingBinlogs[0].filename)
	streamer := exec.Command("bash", "-c", streamerCmd)
	_, err := streamer.Output()
	if err != nil {
		panic(err)
	}
	logger.Error("mysqlbinlog utility quit (maybe the remote server is restarted?)")
	os.Exit(1)
}

func checkMissingBinlogs(config Streamer) []Binlog {
	var match bool
	var missing []Binlog
	for _, r := range config.remoteBinlogs {
		match = false
		for _, l := range config.localBinlogs {
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

func getLocalBinlogs(config Streamer) []Binlog {
    var localBinlogs []Binlog
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

func getRemoteBinlogs(config Streamer) []Binlog {
	var logName string
	var fileSize int64
    var remoteBinlogs []Binlog

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

func configure(configfile string) []Streamer {
	var streamers []Streamer
	cfg, err := ini.Load(configfile)
	if err != nil {
		fmt.Println(err.Error())
	}
	sections := cfg.Sections()
	for _, section := range sections {
		if section.Name() != "DEFAULT" { // skip unnamed section
			portnum, _ := section.Key("mysqlport").Int()
			if portnum == 0 {
				portnum = 3306
			}
			keep, _ := section.Key("keep_days").Int64()
			streamercfg := Streamer{
				streamname:  section.Name(),
				mysqlhost:   section.Key("mysqlhost").String(),
				mysqluser:   section.Key("mysqluser").String(),
				mysqlpass:   section.Key("mysqlpass").String(),
				mysqldb:     section.Key("mysqldb").String(),
				mysqlport:   portnum,
				binlogdir:   section.Key("binlogdir").String(),
				mysqlbinlog: section.Key("mysqlbinlog").String(),
				daysKeep:    keep,
			}
			streamers = append(streamers, streamercfg)
			logger.Notice("Configuration loaded successfully")
		}
	}
	return streamers
}
