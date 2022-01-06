package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"

	_ "github.com/lib/pq"

	"github.com/c-robinson/iplib"
)

const chunkSize int = 20000

var db *sql.DB
var statuses [2]string = [2]string{"online", "offline"}

func main() {
	funPtr := flag.Int("i", 0, "id of implementation to test:\n(1) serial recording\n(2) batch recording\n(3) concurrent batch recording\n(4) concurrent batch recording with suffled inputs")
	flag.Parse()
	if *funPtr == 0 {
		flag.PrintDefaults()
		os.Exit(1)
	}

	rand.Seed(time.Now().UnixNano())

	// Setup postgres connection
	var err error
	db, err = sql.Open("postgres", "host=localhost port=5432 user=postgres password=secret dbname=scans sslmode=disable")
	if err != nil {
		log.Fatal("could not connect to db")
	}
	defer db.Close()

	// Generate IP pool
	// network := iplib.NewNet4(net.ParseIP("192.168.0.0"), 24)
	network := iplib.NewNet4(net.ParseIP("17.0.0.0"), 8)
	ips := network.Enumerate(0, 0)
	// Reduce to 5 million ips for now
	ips = ips[:5000000]

	switch *funPtr {
	case 1:
		// Record statuses one at a time
		useSerialImplementationHandler(ips)
	case 2:
		// Record statuses in batches
		useBatchImplementationHandler(1, 1, ips)
	case 3:
		// Record statuses in batches from two concurrent processors
		go useBatchImplementationHandler(1, 2, ips)
		go useBatchImplementationHandler(2, 2, ips)
	case 4:
		// Record statuses in batches from two concurrent processors, using shuffled inputs
		ips2 := ips
		rand.Shuffle(len(ips2), func(i, j int) {
			ips2[i], ips2[j] = ips2[j], ips2[i]
		})
		fmt.Println("shuffling ips before calling handlers")
		go useBatchImplementationHandler(1, 2, ips)
		go useBatchImplementationHandler(2, 2, ips2)
	}

	// Close on keypress
	var input string
	fmt.Scanln(&input)
}

func useSerialImplementationHandler(ips []net.IP) {
	fmt.Printf("running serial implementation for %d ips:\n", len(ips))

	start := time.Now()
	var status string
	scanQuery := `INSERT INTO ip_statuses (ip, status) VALUES ($1, $2) ON CONFLICT ON CONSTRAINT ip_statuses_pkey DO UPDATE SET status = $3`
	for _, ip := range ips {
		// Generate a random status for each ip
		status = statuses[rand.Intn(2)]

		// Insert / update the row
		_, e := db.Exec(scanQuery, ip.String(), status, status)
		if e != nil {
			log.Fatal("db error: ", e)
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("done in %s\n", elapsed)
}

func useBatchImplementationHandler(processorId int, processorCount int, ips []net.IP) {
	fmt.Printf("running batch implementation [%d of %d] for %d ips:\n", processorId, processorCount, len(ips))

	start := time.Now()
	var status string
	var placeholders []string
	var values []interface{}

	// Break the ip slice up into chunks
	var ipChunks [][]net.IP
	for i := 0; i < len(ips); i += chunkSize {
		end := i + chunkSize
		if end > len(ips) {
			end = len(ips)
		}

		ipChunks = append(ipChunks, ips[i:end])
	}

	for _, ipChunk := range ipChunks {
		// Reset placeholder & value slices
		placeholders = nil
		values = nil

		for indx, ip := range ipChunk {
			placeholders = append(placeholders, fmt.Sprintf("($%d,$%d)",
				indx*2+1,
				indx*2+2,
			))

			// Generate a random status for each ip
			status = statuses[rand.Intn(2)]
			values = append(values, ip.String(), status)
		}

		tx, err := db.Begin()
		if err != nil {
			log.Fatal(err)
		}
		scanQuery := fmt.Sprintf("INSERT INTO ip_statuses (ip, status) VALUES %s ON CONFLICT ON CONSTRAINT ip_statuses_pkey DO UPDATE SET status = excluded.status", strings.Join(placeholders, ","))

		_, err = tx.Exec(scanQuery, values...)
		if err != nil {
			tx.Rollback()
			log.Fatal("db error: ", err)
		}
		err = tx.Commit()
		if err != nil {
			log.Fatal(err)
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("[%d] done in %s\n", processorId, elapsed)
}
