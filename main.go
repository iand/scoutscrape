package main

/*

scoutscrape is a scraper for the Scout Data API (see https://ssd-api.jpl.nasa.gov/doc/scout.html)

Scout provides trajectory analysis and hazard assessment for recently detected objects on the Minor
Planet Center’s Near-Earth Object Confirmation Page (NEOCP)

Some queries:

List objects most likely to hit the earth in the past 12 hours with their velocity in km/s

select object_name, vinf,  max(rating) as max_rating from scout where last_run > now() - interval '12 hours' and rating is not null group by object_name,vinf order by max_rating des
c limit 10;

List closest objects with their brightness and location in sky

select object_name, h, ca_dist, ra, dec from scout where last_run > now() - interval '12 hours' order by ca_dist limit 10;

As above but with url:

select object_name, h, ca_dist, ra, dec, concat('https://cneos.jpl.nasa.gov/scout/#/object/',object_name) as url from scout where last_run > now() - interval '12 hours' order by ca_
dist limit 10;

*/

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"gopkg.in/alecthomas/kingpin.v2"
)

const appname = "scoutscrape"
const minTimeBetweenFetches = 15 * time.Minute

var (
	cachedir    = kingpin.Flag("cachedir", "Name of the directory to cache results in").Default(defaultCacheDir()).String()
	replay      = kingpin.Flag("replay", "Replay data from the disk cache").Default("false").Bool()
	dbname      = kingpin.Flag("dbname", "Name of the database to connect to").Envar("SCOUT_DB_NAME").Default("tsdb").String()
	user        = kingpin.Flag("user", "Name of the database user").Envar("SCOUT_DB_USER").Default("tsdbadmin").String()
	password    = kingpin.Flag("password", "Password of the database user").Envar("SCOUT_DB_PASSWORD").Required().String()
	host        = kingpin.Flag("host", "Hostname of the server to connect to").Envar("SCOUT_DB_HOST").Default("127.0.0.1").String()
	port        = kingpin.Flag("port", "Port of the server to connect to").Envar("SCOUT_DB_PORT").Default("30000").String()
	connectOpts = kingpin.Flag("dbopts", "Space separated list of additional database connection options").Envar("SCOUT_DB_OPTS").Default("sslmode=require").String()
)

func defaultCacheDir() string {
	d, err := os.UserCacheDir()
	if err != nil {
		panic(err.Error())
	}
	return filepath.Join(d, appname)
}

func main() {
	kingpin.Parse()
	log.Printf("using cache directory is %s", *cachedir)

	if err := Main(); err != nil {
		log.Fatal(err.Error())
	}
}

func Main() error {
	if *replay {
		return replayFromCache()
	}

	// Check cache to see if we should fetch from API or not
	recent, err := cacheModifiedSince(time.Now().Add(-minTimeBetweenFetches))
	if err != nil {
		return fmt.Errorf("cache: %w", err)
	}
	if recent {
		log.Printf("nothing to do, already fetched recently")
		return nil
	}

	u := "https://ssd-api.jpl.nasa.gov/scout.api"

	res, err := http.Get(u)
	if err != nil {
		return fmt.Errorf("fetch: %w", err)
	}
	if res.StatusCode != 200 {
		return fmt.Errorf("fetch: bad response %s", res.Status)
	}

	cacheFile, err := newCacheFile()
	if err != nil {
		return fmt.Errorf("cache: %w", err)
	}
	defer cacheFile.Close()

	tee := io.TeeReader(res.Body, cacheFile)

	var summary Summary

	err = json.NewDecoder(tee).Decode(&summary)
	if err != nil {
		return fmt.Errorf("decode: %w", err)
	}

	if summary.Signature.Version != "1.2" {
		return fmt.Errorf("summary: unknown version found: %v", summary.Signature.Version)
	}

	return writeSummaries([]Summary{summary})
}

func cacheModifiedSince(t time.Time) (bool, error) {
	if _, err := os.Stat(*cachedir); os.IsNotExist(err) {
		return false, nil
	}

	files, err := ioutil.ReadDir(*cachedir)
	if err != nil {
		return false, err
	}

	for _, fi := range files {
		if !fi.Mode().IsRegular() {
			continue
		}
		if fi.ModTime().After(t) {
			return true, nil
		}
	}

	return false, nil
}

func newCacheFile() (*os.File, error) {
	if _, err := os.Stat(*cachedir); os.IsNotExist(err) {
		err := os.MkdirAll(*cachedir, 0700)
		if err != nil {
			return nil, err
		}
	}

	filename := filepath.Join(*cachedir, fmt.Sprintf("%d.json", time.Now().Unix()))
	return os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
}

func replayFromCache() error {
	if _, err := os.Stat(*cachedir); os.IsNotExist(err) {
		return nil // nothing to do
	}

	files, err := ioutil.ReadDir(*cachedir)
	if err != nil {
		return err
	}

	summaries := make([]Summary, 0, len(files))

	for _, fi := range files {
		if !fi.Mode().IsRegular() {
			continue
		}

		f, err := os.Open(filepath.Join(*cachedir, fi.Name()))
		if err != nil {
			return err
		}

		var summary Summary

		err = json.NewDecoder(f).Decode(&summary)
		if err != nil {
			return fmt.Errorf("decode: failed to decode %s: %w", fi.Name(), err)
		}

		if summary.Signature.Version != "1.2" {
			log.Printf("summary: unknown version found in summary %s: %v", fi.Name(), summary.Signature.Version)
			continue
		}

		summaries = append(summaries, summary)
	}

	log.Printf("replaying %d summaries", len(summaries))

	return writeSummaries(summaries)
}

func buildConnectOptions() string {
	var opts []string
	if *host != "" {
		opts = append(opts, "host="+*host)
	}
	if *port != "" {
		opts = append(opts, "port="+*port)
	}
	if *user != "" {
		opts = append(opts, "user="+*user)
	}
	if *password != "" {
		opts = append(opts, "password="+*password)
	}
	if *dbname != "" {
		opts = append(opts, "dbname="+*dbname)
	}
	if *connectOpts != "" {
		opts = append(opts, *connectOpts)
	}

	return strings.Join(opts, " ")
}

func writeSummaries(summaries []Summary) error {
	opts := buildConnectOptions()
	db, err := sqlx.Connect("postgres", opts)
	if err != nil {
		return fmt.Errorf("connect: %v", err)
	}
	defer db.Close()

	// Ensure we have the table
	if _, err := db.Exec(schema); err != nil {
		return fmt.Errorf("schema: %v", err)
	}

	stmt, err := db.Prepare(`INSERT INTO scout (
		object_name, last_run, h, rating, ca_dist, moid, neo_score, neo_1km_score, pha_score, ieo_score, 
		geocentric_score, tisserand_score, unc, uncp1, ra, dec, elong, tephem, rate, nobs, 
		arc, vinf, rmsn, vmag 
	) VALUES (
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
		$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
		$21,$22,$23,$24
	)  ON CONFLICT DO NOTHING`)
	if err != nil {
		return fmt.Errorf("prepare: %v", err)
	}

	candidates := 0
	inserted := 0

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("transaction: %v", err)
	}
	for _, s := range summaries {
		for _, d := range s.Data {
			candidates++
			res, err := stmt.Exec(
				d.ObjectName,
				time.Time(d.LastRun),
				d.H.V(),
				d.Rating.V(),
				d.CaDist.V(),
				d.Moid.V(),
				d.NeoScore.V(),
				d.Neo1KmScore.V(),
				d.PhaScore.V(),
				d.IeoScore.V(),
				d.GeocentricScore.V(),
				d.TisserandScore.V(),
				d.Unc.V(),
				d.UncP1.V(),
				d.Ra,
				d.Dec,
				d.Elong,
				time.Time(d.TEphem),
				d.Rate.V(),
				d.NObs.V(),
				d.Arc.V(),
				d.VInf.V(),
				d.RmsN.V(),
				d.Vmag.V(),
			)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("exec: %v", err)
			}
			n, _ := res.RowsAffected()
			inserted += int(n)
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf("added %d observations (%d duplicates ignored)", inserted, candidates-inserted)

	return nil
}
