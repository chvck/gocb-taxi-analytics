package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/couchbase/gocb"
	"github.com/gorilla/mux"
)

var (
	cbConnStr       = "couchbase://localhost"
	cbAnalyticsNode = "localhost:8095"
	cbUsername      = "user"
	cbPassword      = "password"
	cluster         *gocb.Cluster
)

func routes() *mux.Router {
	router := mux.NewRouter()

	router.HandleFunc("/", index).Methods("GET")
	router.HandleFunc("/all", requestHandler).Methods("GET")
	s := http.StripPrefix("/static/", http.FileServer(http.Dir("./static/")))
	router.PathPrefix("/static/").Handler(s).Methods("GET")

	return router
}

func run() (*http.Server, error) {
	r := routes()

	srv := &http.Server{Addr: "localhost:8010", Handler: r}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("Error running server: %s", err)
		}
	}()

	return srv, nil
}

func runServer() {
	var err error
	cluster, err = gocb.Connect(cbConnStr)
	if err != nil {
		panic("Error connecting to cluster:" + err.Error())
	}

	cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: cbUsername,
		Password: cbPassword,
	})

	cluster.EnableAnalytics([]string{cbAnalyticsNode})

	stop := make(chan os.Signal, 1)

	// Stop the server on interrupt
	signal.Notify(stop, os.Interrupt)

	srv, err := run()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Server running on", srv.Addr)
	<-stop
	log.Println("Stopping server")
	srv.Shutdown(nil)
}

func main() {
	reformat := flag.Bool("reformat", false, "Reformat the taxis csv file")
	path := flag.String("csv", "", "Path to the the taxis csv file for reformatting")
	flag.Parse()

	if *reformat {
		if *path == "" {
			panic("path must be used if reformat is set")
		}

		processData(*path)
	} else {
		runServer()
	}
}

func index(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/static/", http.StatusFound)
}

// processResults iterates through the analytics results and pulls out
// the time periods and aggregated data for each time period.
func processResults(results gocb.AnalyticsResults) (*calendarData, error) {
	var row map[string]interface{}
	var dateParts []float64
	var aggregates []float64
	for results.Next(&row) {
		if datePart, ok := row["period"]; ok {
			dateParts = append(dateParts, datePart.(float64))
			aggregates = append(aggregates, row["aggregate"].(float64))
		}
	}

	// Ensure that we've read all of the data correctly.
	if err := results.Close(); err != nil {
		return nil, err
	}

	return &calendarData{
		DateParts: dateParts,
		Aggregate: aggregates,
	}, nil
}

type calendarData struct {
	DateParts []float64 `json:"periods"`
	Aggregate []float64 `json:"aggregate"`
	Where     string    `json:"where"`
}

// whereTimePeriod creates where clauses for time bounding
// the analytics query.
func whereTimePeriod(period string, query url.Values) string {
	where := ""
	if period == "day" {
		month := query["month"][0]
		if month == "1" {
			where = `pickupDate <= "2016-01-31 23:59:59"`
		} else if month == "12" {
			where = `pickupDate >= "2016-12-01 00:00:00"`
		} else {
			monthInt, _ := strconv.ParseFloat(month, 64)
			where = fmt.Sprintf(`pickupDate >= "2016-%02g-01T00:00:00" AND pickupDate <= "2016-%02g-31T23:59:59"`, monthInt, monthInt)
		}
	} else if period == "hour" {
		month := query["month"][0]
		monthInt, _ := strconv.ParseFloat(month, 64)
		day := query["day"][0]
		dayInt, _ := strconv.ParseFloat(day, 64)
		where = fmt.Sprintf(`pickupDate > "2016-%02g-%02gT00:00:00" AND pickupDate <= "2016-%02g-%02gT23:59:59"`, monthInt, dayInt, monthInt, dayInt)
	}

	return where
}

// processQueryString extracts the parameters that the frontend
// has sent.
// e.g. period=hour&month=5&day=14&aggregate=count(*)&where=fareAmount>15&where=tip<1
func processQueryString(queryString url.Values) (string, string, string) {
	aggregate := queryString["aggregate"][0]
	period := "month"
	where := ""
	if len(queryString["period"]) > 0 {
		period = queryString["period"][0]
		where = whereTimePeriod(period, queryString)
	}

	for _, cond := range queryString["where"] {
		if len(where) > 0 {
			where = fmt.Sprintf("%s AND %s", where, cond)
		} else {
			where = fmt.Sprintf("%s", cond)
		}
	}

	if len(where) > 0 {
		where = fmt.Sprintf("WHERE %s", where)
	}

	return aggregate, where, period
}

// requestHandler handles requests to the /all endpoint.
// It processes the querystring extracting parameters,
// runs an analytics query, processes the results and
// then sends a response.
func requestHandler(w http.ResponseWriter, r *http.Request) {
	aggregate, where, period := processQueryString(r.URL.Query())

	// We use string formatting here but in the future we'll be able to use
	// parameterized queries.
	q := `select DATE_PART_STR(pickupDate, "%s") AS period, %s as aggregate FROM alltaxis`
	q += `%s GROUP BY DATE_PART_STR(pickupDate, "%s") ORDER BY period;`
	query := gocb.NewAnalyticsQuery(fmt.Sprintf(q, period, aggregate, where, period))
	results, err := cluster.ExecuteAnalyticsQuery(query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data, err := processResults(results)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data.Where = fmt.Sprintf("%s %s", aggregate, where)
	js, err := json.Marshal(*data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(200)
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

// processData processes the data in the original
// taxi dataset csv and updates the dates to
// work with Analytics, a step necessary at time of
// writing.
func processData(path string) {
	destFile, err := os.Create("2016_Green_Taxi_Trip_Data.csv")
	if err != nil {
		panic(err.Error())
	}
	defer destFile.Close()
	csvFile, err := os.Open(path)
	if err != nil {
		panic(err.Error())
	}
	defer csvFile.Close()

	wr := csv.NewWriter(destFile)
	defer wr.Flush()

	reader := csv.NewReader(bufio.NewReader(csvFile))
	layout := "01/02/2006 15:04:05"

	headers := []string{
		"vendorID",
		"pickupDate",
		"dropoffDate",
		"storeFlag",
		"rateCode",
		"pickupLon",
		"pickupLat",
		"dropoffLon",
		"dropoffLat",
		"passengers",
		"tripDistance",
		"fareAmount",
		"extra",
		"mta",
		"tip",
		"tolls",
		"ehail",
		"improvement",
		"total",
		"paymentType",
		"tripType",
		"pickupLocation",
		"dropoffLocation",
		"type",
	}

	err = wr.Write(headers)
	if err != nil {
		panic(err.Error())
	}
	reader.Read()
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err.Error())
		}

		t := []string{
			(line[0]),
			line[1],
			line[2],
			line[3],
			(line[4]),
			(line[5]),
			(line[6]),
			(line[7]),
			(line[8]),
			(line[9]),
			(line[10]),
			(line[11]),
			(line[12]),
			(line[13]),
			(line[14]),
			(line[15]),
			(line[16]),
			(line[17]),
			(line[18]),
			(line[19]),
			(line[20]),
			line[21],
			line[22],
			"green",
		}

		pickup, err := time.Parse(layout, t[1][0:len(t[1])-3])
		if err != nil {
			panic(err.Error())
		}

		dropoff, err := time.Parse(layout, t[2][0:len(t[2])-3])
		if err != nil {
			panic(err.Error())
		}

		pickupHour := pickup.Hour()
		if t[1][len(t[1])-2:] == "PM" {
			if pickupHour != 12 {
				pickupHour = pickupHour + 12
			}
		} else if pickupHour == 12 {
			pickupHour = 0
		}

		dropoffHour := dropoff.Hour()
		if t[2][len(t[1])-2:] == "PM" {
			if dropoffHour != 12 {
				dropoffHour = dropoffHour + 12
			}
		} else if dropoffHour == 12 {
			dropoffHour = 0
		}

		t[1] = fmt.Sprintf("%d-%02d-%02dT%02d:%02d:%02d", pickup.Year(), pickup.Month(), pickup.Day(),
			pickupHour, pickup.Minute(), pickup.Second())

		t[2] = fmt.Sprintf("%d-%02d-%02dT%02d:%02d:%02d", dropoff.Year(), dropoff.Month(), dropoff.Day(),
			dropoffHour, dropoff.Minute(), dropoff.Second())

		err = wr.Write(t)
		if err != nil {
			panic(err.Error())
		}
	}
}
