package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/ini.v1"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strconv"
)

type Configuration struct {
	mysqlhost   string // MySQL host to connect, if empty local socket will be used
	mysqluser   string // User to connect MySQL with
	mysqlpass   string // Password for connecting MySQL
	mysqldb     string // Database to connect to
	mysqlport   int    // Port to connect MySQL, if left blank, 3306 will be used as default
	binlogdir   string // Directory to keep binlogs
	mysqlbinlog string // mysqlbinlog binary with full path
}

type Binlog struct {
	filename string
	filesize int64
}

var (
	remoteBinlogs  []Binlog
	localBinlogs   []Binlog
	missingBinlogs []Binlog
)

func main() {
	configfile := flag.String("cfg", "binlogstreamer.cfg", "Configuration file")
	flag.Parse()
	config := configure(*configfile)
	remoteBinlogs := getRemoteBinlogs(config)
	localBinlogs := getLocalBinlogs(config)
	missingBinlogs := checkMissingBinlogs(config, localBinlogs, remoteBinlogs)
	streamBinlogs(config, missingBinlogs)
	for {
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
	streamer := exec.Command("bash", "-c", streamerCmd)
	_, err := streamer.Output()
	if err != nil {
		panic(err)
	}
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

	connecturi := fmt.Sprint(config.mysqluser, ":", config.mysqlpass, "@tcp(", config.mysqlhost, ":", config.mysqlport, ")/", config.mysqldb)
	db, err := sql.Open("mysql", connecturi)
	if err != nil {
		fmt.Println(err.Error())
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
	retcfg := Configuration{
		mysqlhost:   cfg.Section("DEFAULT").Key("mysqlhost").String(),
		mysqluser:   cfg.Section("DEFAULT").Key("mysqluser").String(),
		mysqlpass:   cfg.Section("DEFAULT").Key("mysqlpass").String(),
		mysqldb:     cfg.Section("DEFAULT").Key("mysqldb").String(),
		mysqlport:   portnum,
		binlogdir:   cfg.Section("DEFAULT").Key("binlogdir").String(),
		mysqlbinlog: cfg.Section("DEFAULT").Key("mysqlbinlog").String(),
	}

	return &retcfg
}
