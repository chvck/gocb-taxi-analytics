package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/couchbase/gocb"
	"github.com/gorilla/mux"
)

var cluster *gocb.Cluster

func routes() *mux.Router {
	router := mux.NewRouter()

	router.HandleFunc("/", index).Methods("GET")
	router.HandleFunc("/all", allData).Methods("GET")
	router.HandleFunc("/one", oneData).Methods("GET")
	s := http.StripPrefix("/static/", http.FileServer(http.Dir("./static/")))
	router.PathPrefix("/static/").Handler(s).Methods("GET")

	return router
}

func run() (*http.Server, error) {
	r := routes()

	address := fmt.Sprintf("%v:%v", "localhost", "8080")

	srv := &http.Server{Addr: address, Handler: r}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("Error running server: %s", err)
		}
	}()

	return srv, nil
}

func main() {
	var err error
	cluster, err = gocb.Connect("couchbase://localhost")
	if err != nil {
		panic("ERROR CONNECTING TO CLUSTER:" + err.Error())
	}

	cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: "Administrator",
		Password: "password",
	})

	cluster.EnableAnalytics([]string{"localhost:8095"})
	cluster.SetAnalyticsTimeout(100 * time.Second)

	stop := make(chan os.Signal, 1)

	signal.Notify(stop, os.Interrupt)

	srv, err := run()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(srv.Addr)
	<-stop
	log.Println("Stopping server")
	srv.Shutdown(nil)
}

func index(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/static/", http.StatusFound)
}

func oneData(w http.ResponseWriter, r *http.Request) {
	vendor := r.URL.Query()["vendor"][0]
	whereQ := r.URL.Query()["where"]
	monthStr := r.URL.Query()["month"][0]
	month, _ := strconv.ParseFloat(monthStr, 64)
	where := ""
	if month == 1 {
		where = `pickupDate <= "2016-01-31 23:59:59"`
	} else if month == 12 {
		where = `pickupDate > "2016-12-01 00:00:00"`
	} else {
		where = fmt.Sprintf(`pickupDate > "2016-%02g-01 00:00:00" AND pickupDate <= "2016-%02g-31 23:59:59"`, month, month)
	}
	if len(whereQ) > 0 {
		where = where + " AND " + whereQ[0]
	}
	queryTxt := fmt.Sprintf(`select DATE_PART_STR(pickupDate, 'day') AS day, COUNT(*) as count FROM
	vendor%staxis WHERE %s GROUP BY DATE_PART_STR(pickupDate, 'day');`, vendor, where)
	query := gocb.NewAnalyticsQuery(queryTxt)
	results, err := cluster.ExecuteAnalyticsQuery(query)
	if err != nil {
		panic("ERROR EXECUTING ANALYTICS:" + err.Error())
	}

	var row map[string]interface{}
	var days []float64
	var counts []float64
	// var fares []float64
	// var tips []float64
	for results.Next(&row) {
		if day, ok := row["day"]; ok {
			days = append(days, day.(float64))
			counts = append(counts, row["count"].(float64))
			// tips = append(tips, row["tips"].(float64))
			// fares = append(fares, row["fares"].(float64))
		}
	}
	if err = results.Close(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	data := calendarData{
		Periods: days,
		Counts:  counts,
		Where:   where,
		// Fares: fares,
		// Tips:  tips,
	}
	js, err := json.Marshal(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	w.WriteHeader(200)
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)

}

func allData(w http.ResponseWriter, r *http.Request) {
	vendor := r.URL.Query()["vendor"][0]
	where := r.URL.Query()["where"]
	whereTxt := ""
	if len(where) > 0 {
		whereTxt = "WHERE " + where[0]
	}
	query := gocb.NewAnalyticsQuery(fmt.Sprintf(`select DATE_PART_STR(pickupDate, 'month') AS month, COUNT(*) as count FROM
	vendor%staxis %s GROUP BY DATE_PART_STR(pickupDate, 'month');`, vendor, whereTxt))
	results, err := cluster.ExecuteAnalyticsQuery(query)
	if err != nil {
		panic("ERROR EXECUTING ANALYTICS:" + err.Error())
	}

	var row map[string]interface{}
	var months []float64
	var counts []float64
	// var fares []float64
	// var tips []float64
	for results.Next(&row) {
		if month, ok := row["month"]; ok {
			months = append(months, month.(float64))
			counts = append(counts, row["count"].(float64))
			// tips = append(tips, row["tips"].(float64))
			// fares = append(fares, row["fares"].(float64))
		}
	}
	if err = results.Close(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	data := calendarData{
		Periods: months,
		Counts:  counts,
		// Fares: fares,
		// Tips:  tips,
		Where: whereTxt,
	}
	js, err := json.Marshal(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	w.WriteHeader(200)
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)

}

type calendarData struct {
	Periods []float64 `json:"periods"`
	Counts  []float64 `json:"counts"`
	Fares   []float64 `json:"fares"`
	Tips    []float64 `json:"tips"`
	Where   string    `json:"where"`
}

type trip struct {
	VendorID     uint    `json:"vendor_id,omitempty"`
	PickupDate   string  `json:"pickup_date,omitempty"`
	DropoffDate  string  `json:"dropoff_date,omitempty"`
	StoreFlag    string  `json:"store_flag,omitempty"`
	RateCode     uint    `json:"rate_code,omitempty"`
	PickupLon    float64 `json:"pickup_lon,omitempty"`
	PickupLat    float64 `json:"pickup_lat,omitempty"`
	DropoffLon   float64 `json:"dropoff_lon,omitempty"`
	DropoffLat   float64 `json:"dropoff_lat,omitempty"`
	Passengers   uint    `json:"passengers,omitempty"`
	TripDistance float64 `json:"trip_distance,omitempty"`
	FareAmount   float64 `json:"fare_amount,omitempty"`
	Extra        float64 `json:"extra,omitempty"`
	MTA          float64 `json:"mta,omitempty"`
	Tip          float64 `json:"tip,omitempty"`
	Tolls        float64 `json:"tolls,omitempty"`
	Ehail        float64 `json:"ehail,omitempty"`
	Improvement  float64 `json:"improvement,omitempty"`
	Total        float64 `json:"total,omitempty"`
	PaymentType  uint    `json:"payment_type,omitempty"`
	TripType     uint    `json:"trip_type,omitempty"`
	PULocation   string  `json:"pickup_location,omitempty"`
	DOLocation   string  `json:"dropoff_location,omitempty"`
	TaxiType     string  `json:"type,omitempty"`
}
