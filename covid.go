package main

import(
	"os"
	"log"
	"io/ioutil"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

type InputRecord struct {
	Country string
	Province string
	Status string
	Cases int
	Date string
	Confirmed int
	Deaths int
	Recovered int
}

type OutputRecord struct {
	Country string
	Date string
	Confirmed int
	NewConfirmed int
	Deaths int
	NewDeaths int
	Recovered int
	NewRecovered int
	Day int
	Active int
}

func main() {
	// Read data
	var err error
	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatalf("Couldn't open file: %s", err)
	}

	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalf("Couldn't read file: %s", err)
	}

	var data []InputRecord
	err = json.Unmarshal(bytes, &data)
	if err != nil {
		log.Fatalf("Couldn't parse json: %s", err)
	}

	// Summarize data
	countries := map[string]bool{}
	dates := map[string]bool{}
	counters := map[string]int{}
	for _, entry := range data {
		dkey := fmt.Sprintf("%s%s%s-day", entry.Country, entry.Date, entry.Status)
		counters[dkey] = counters[dkey] + entry.Cases
		countries[entry.Country] = true
		dates[entry.Date] = true
	}

	// Sort dates
	sortedDates := []string{}
	for k := range dates {
		sortedDates = append(sortedDates, k)
	}
	sort.Strings(sortedDates)

	// Sort countries
	sortedCountries := []string{}
	for k := range countries {
		sortedCountries = append(sortedCountries, k)
	}
	sort.Strings(sortedCountries)

	// Build output records
	var out []OutputRecord
	days := map[string]int{}
	for _, country := range sortedCountries {
		for _, date := range sortedDates {

			tmp := OutputRecord{
				Country: country,
				Date: date,
				Confirmed: counters[fmt.Sprintf("%s%s%s-day", country, date, "confirmed")],
				Deaths: counters[fmt.Sprintf("%s%s%s-day", country, date, "deaths")],
				Recovered: counters[fmt.Sprintf("%s%s%s-day", country, date, "recovered")],
			}

			tmp.Active = tmp.Confirmed - tmp.Recovered - tmp.Deaths

			dtDate, _ := time.Parse(time.RFC3339, date)
			dtDate = dtDate.Add(time.Duration(-24) * time.Hour)

			if v, ok := counters[fmt.Sprintf("%s%s%s-day", country, dtDate.Format(time.RFC3339), "confirmed")]; ok {
				tmp.NewConfirmed = tmp.Confirmed - v
			}

			if v, ok := counters[fmt.Sprintf("%s%s%s-day", country, dtDate.Format(time.RFC3339), "deaths")]; ok {
				tmp.NewDeaths = tmp.Deaths - v
			}

			if v, ok := counters[fmt.Sprintf("%s%s%s-day", country, dtDate.Format(time.RFC3339), "recovered")]; ok {
				tmp.NewRecovered = tmp.Recovered - v
			}

			if tmp.Confirmed < 10 {
				continue
			}

			// Increment & set days since threshold (10) counter
			days[tmp.Country] += 1
			tmp.Day = days[tmp.Country]

			out = append(out, tmp)
		}
	}

	// Output
	if os.Args[2] == "csv" {
		fmt.Printf("Date,Day,Country,Confirmed,Deaths,Recovered,NewConfirmed,NewDeaths,NewRecovered,Active\n")
		for _, tmp := range out {
			fmt.Printf("%s,%d,%s,%d,%d,%d,%d,%d,%d,%d\n", tmp.Date, tmp.Day, tmp.Country, tmp.Confirmed, tmp.Deaths, tmp.Recovered, tmp.NewConfirmed, tmp.NewDeaths, tmp.NewRecovered, tmp.Active)
		}
	} else if os.Args[2] == "esbulk" {
		for id, tmp := range out {
			fmt.Printf("{\"index\": {\"_id\": %d } }\n", id)
			bytesOut, _ := json.Marshal(tmp)
			fmt.Println(string(bytesOut))
		}
	} else {
		bytesOut, _ := json.Marshal(out)
		fmt.Print(string(bytesOut))
	}
}
