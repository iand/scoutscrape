package main

import (
	"bytes"
	"strconv"
	"time"
)

var null = []byte("null")

type Summary struct {
	Count     string `json:"count"`
	Signature struct {
		Source  string `json:"source"`
		Version string `json:"version"`
	} `json:"signature"`
	Data []Detail `json:"data"`
}

type Detail struct {
	ObjectName      string  `json:"objectName"`      // NEOCP temporary object designation
	LastRun         mintime `json:"lastRun"`         // Date/time of last object analysis computation (UTC)
	H               qfloat  `json:"H"`               // Absolute magnitude
	Rating          qint    `json:"rating"`          // A rating to characterize the chances of an Earth impact (0=negligible, 1=small, 2=modest, 3=moderate, 4=elevated)
	CaDist          qfloat  `json:"caDist"`          // Close-approach distance to Earth in lunar distances (LD)
	Moid            qfloat  `json:"moid"`            // Minimum distance between the orbits of Earth and the object (au)
	NeoScore        qint    `json:"neoScore"`        // Score for the object being a NEO (0-100)
	Neo1KmScore     qint    `json:"neo1kmScore"`     // Score for the object being an NEO larger than 1 km in diameter
	PhaScore        qint    `json:"phaScore"`        // Score for the object being a PHA (0-100)
	IeoScore        qint    `json:"ieoScore"`        // Score for the object being an IEO, Interior Earth Object (0-100)
	GeocentricScore qint    `json:"geocentricScore"` // Score for the object having a geocentric orbit (0-100)
	TisserandScore  qint    `json:"tisserandScore"`  // Score for the object having a Jupiter tisserand invariant less than 3, comet-like orbit (0-100)
	Unc             qfloat  `json:"unc"`             // 1-sigma uncertainty in the plane-of-sky of the current ephemeris (arc-minutes)
	UncP1           qfloat  `json:"uncP1"`           // 1-sigma uncertainty in the plane-of-sky one day after the current ephemeris (arc-minutes)
	Ra              string  `json:"ra"`              // Right Ascension of the current ephemeris (hh:mm, J2000)
	Dec             string  `json:"dec"`             // Declination of the current ephemeris (degrees, J2000)
	Elong           string  `json:"elong"`           // Solar elongation of the current ephemeris (degrees)
	TEphem          mintime `json:"tEphem"`          // Date/time of ephemeris computation (UTC)
	Rate            qfloat  `json:"rate"`            // Plane-of-sky rate of motion (arc-seconds per minute)
	NObs            qint    `json:"nObs"`            // Number of observations available
	Arc             qfloat  `json:"arc"`             // Length of the observation data arc in hours
	VInf            qfloat  `json:"vInf"`            // Asymptotic velocity relative to Earth (when defined)
	RmsN            qfloat  `json:"rmsN"`            // RMS of the weighted residuals for the best fitting orbit
	Vmag            qfloat  `json:"Vmag"`            // V-band magnitude estimate of the current ephemeris (mag)
}

type qint struct{ *int } // nullable

func (i *qint) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, null) {
		return nil
	}
	v, err := strconv.Atoi(unquote(data))
	if err != nil {
		return err
	}
	*i = qint{&v}
	return nil
}

func (i *qint) V() interface{} {
	if i != nil && i.int != nil {
		return int(*i.int)
	}
	return nil
}

type qfloat struct{ *float64 } // nullable

func (f *qfloat) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, null) {
		return nil
	}
	v, err := strconv.ParseFloat(unquote(data), 64)
	if err != nil {
		return err
	}
	*f = qfloat{&v}
	return nil
}

func (f *qfloat) V() interface{} {
	if f != nil && f.float64 != nil {
		return float64(*f.float64)
	}
	return nil
}

func unquote(data []byte) string {
	if len(data) > 1 && data[0] == data[len(data)-1] && (data[0] == '\'' || data[0] == '"') {
		return string(data[1 : len(data)-1])
	}
	return string(data)
}

type mintime time.Time // time to nearest minute

func (t *mintime) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, null) {
		return nil
	}

	v, err := time.ParseInLocation("2006-01-02 15:04", unquote(data), time.UTC)
	if err != nil {
		return err
	}
	*t = mintime(v)
	return nil
}

var schema = `
CREATE TABLE IF NOT EXISTS scout (
	object_name	      TEXT NOT NULL,
	last_run          TIMESTAMPTZ NOT NULL,
	h                 REAL,
	rating            SMALLINT,
	ca_dist           REAL,
	moid              REAL,
	neo_score         SMALLINT,
	neo_1km_score     SMALLINT,
	pha_score         SMALLINT,
	ieo_score         SMALLINT,
	geocentric_score  SMALLINT,
	tisserand_score   SMALLINT,
	unc               REAL,
	uncp1             REAL,
	ra                TEXT,
	dec               TEXT,
	elong             TEXT,
	tephem            TIMESTAMPTZ,
	rate              REAL,
	nobs              SMALLINT,
	arc               REAL,
	vinf              REAL,
	rmsn              REAL,
	vmag              REAL,
	PRIMARY KEY(object_name, last_run)
);

SELECT create_hypertable('scout', 'last_run', if_not_exists => true);
`
