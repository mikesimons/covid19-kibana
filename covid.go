package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/go-getter"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Record struct {
	Date time.Time
	Country string
	Region string
	Confirmed int
	Deaths int
	Recovered int
	Active int
	Calculated map[string]int
}

func main() {
	from, _ := time.Parse("2006-01-02", "2020-01-22")
	to := time.Now().Add(-24 * time.Hour)

	var records []Record
	err := eachDay(from, to, func(d time.Time) error {
		url := urlForDate(d)
		file := fileForDate(d)

		// Fetch file if we don't already have it
		// Assumes that remote files are immutable
		if _, err := os.Stat(file); os.IsNotExist(err) {
			err := getter.GetFile(file, url)
			if err != nil {
				return fmt.Errorf("error fetching '%s': %w", url, err)
			}
		}

		// Open data file for reading
		fh, _ := os.Open(file)
		defer fh.Close()
		buf := bufio.NewReader(fh)

		// Loop all rows
		var mapper func(string)Record
		for {
			row, err := buf.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("error reading %s: %w", file, err)
			}

			// If we don't have a mapper already, try to create one
			if mapper == nil {
				mapper, err = rowMapper(row)
				if err != nil {
					log.Fatal(fmt.Errorf("unable to create row mapper: %w", err))
				}
			}

			// Map row to record, augmenting with date
			record := mapper(row)
			record.Date = d
			aggregate(&record)

			records = append(records, record)
		}

		return nil
	})

	if err != nil {
		log.Fatalf("unknown error: %s", err)
	}

	for i, record := range records {
		applyAggregates(&record)
		fmt.Printf("{\"index\": {\"_id\": %d } }\n", i)
		out, _ := json.Marshal(record)
		fmt.Println(string(out))
	}
}

// eachDay loops over days between the bounds and calls the callback function
func eachDay(from time.Time, to time.Time, fn func(time.Time) error) error {
	current := from
	for ; !current.After(to); current = current.AddDate(0, 0, 1) {
		err := fn(current)
		if err != nil {
			return err
		}
	}
	return nil
}

// fileForDate gets the local path of the file for a given date
func fileForDate(d time.Time) (string) {
	return fmt.Sprintf("data/%s.csv",  d.Format("01-02-2006"))
}

// urlForDate gets the remote URL of the file for a given date
func urlForDate(d time.Time) (string) {
	return fmt.Sprintf(
		"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/%s.csv",
		d.Format("01-02-2006"),
	)
}

// rowMapper takes a header row and produces a mapping function that will populate a Record with the correct fields
// This is necessary because the format of the daily reports has changed over time
func rowMapper(headerRow string) (func(string) Record, error) {
	headerRow = strings.ToLower(headerRow)
	headerRow = strings.ReplaceAll(headerRow, "/", "_")
	headerRow = strings.ReplaceAll(headerRow, "\"", "")

	fields := strings.Split(headerRow, ",")
	fieldMap := map[string]int{}
	for index, header := range fields {
		if regexp.MustCompile(".*country.*").MatchString(header) {
			fieldMap["country"] = index
			continue
		}

		if regexp.MustCompile(".*region|province.*").MatchString(header) {
			fieldMap["region"] = index
			continue
		}

		if regexp.MustCompile(".*confirm.*").MatchString(header) {
			fieldMap["confirmed"] = index
			continue
		}

		if regexp.MustCompile(".*death.*").MatchString(header) {
			fieldMap["deaths"] = index
			continue
		}

		if regexp.MustCompile(".*recover.*").MatchString(header) {
			fieldMap["recovered"] = index
			continue
		}
	}

	// Make sure we've got all of the fields
	if len(fieldMap) != 5 {
		return nil, fmt.Errorf("missing field mappings: %#v", fieldMap)
	}

	return func(row string) Record {
		// We're assuming this data is relatively simple and isn't going to have nested commas
		row = strings.ReplaceAll(row, "\"", "")
		row = strings.TrimSpace(row)
		fields = strings.Split(row, ",")
		confirmed, _ := strconv.Atoi(fields[fieldMap["confirmed"]])
		deaths, _ := strconv.Atoi(fields[fieldMap["deaths"]])
		recovered, _ := strconv.Atoi(fields[fieldMap["recovered"]])

		country := fields[fieldMap["country"]]
		if country == "Mainland China" {
			country = "China"
		}

		return Record {
			Country: country,
			Region: fields[fieldMap["region"]],
			Confirmed: confirmed,
			Deaths: deaths,
			Recovered: recovered,
			Active: confirmed - deaths - recovered,
			Calculated: make(map[string]int),
		}
	}, nil
}

var counters map[string]int
var thresholds map[string]*time.Time

func aggregate(r *Record) {
	if thresholds == nil {
		thresholds = make(map[string]*time.Time)
		counters = make(map[string]int)
	}

	key := r.Country + r.Region + r.Date.Format("2006-02-01")

	counters[key + "confirmed"] += r.Confirmed
	counters[key + "deaths"] += r.Deaths
	counters[key + "recovered"] += r.Recovered
	counters[key + "active"] += r.Active

	thresholds := func(name string, limits []int) {
		for _, v := range limits {
			tkey := fmt.Sprintf("%s%d%s", name, v, r.Country)
			if counters[key + name] > v && thresholds[tkey] == nil {
				thresholds[tkey] = &r.Date
			}
		}
	}

	thresholds("confirmed", []int{10, 100})
	thresholds("deaths", []int{10, 100})
}

func applyAggregates(r *Record) {
	thresholds := func(key string, name string) {
		if v := thresholds[key + r.Country]; v != nil {
			days := int(r.Date.Sub(*v).Hours()/24)
			if days >= 0 {
				r.Calculated[name] = days
			}
		}
	}
	thresholds("confirmed10", "DaysSince10Confirmed")
	thresholds("confirmed100", "DaysSince100Confirmed")
	thresholds("deaths10", "DaysSince10Deaths")
	thresholds("deaths100", "DaysSince100Deaths")

	deltas := func(t string, name string) {
		yesterday := r.Date.AddDate(0, 0, -1)
		key := r.Country + r.Region + r.Date.Format("2006-02-01") + t
		prevKey := r.Country + r.Region + yesterday.Format("2006-02-01") + t

		if _, ok := counters[prevKey]; !ok {
			r.Calculated[name] = 0
		} else {
			r.Calculated[name] = counters[key] - counters[prevKey]
		}
	}
	deltas("confirmed", "ConfirmedDelta")
	deltas("deaths", "DeathsDelta")
	deltas("recovered", "RecoveredDelta")
}