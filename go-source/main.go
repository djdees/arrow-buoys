package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/ipc"
	"github.com/apache/arrow/go/v16/arrow/memory"

	parquet "github.com/parquet-go/parquet-go"
)

// MetRow must mirror the schema written by go-ingest.
type MetRow struct {
	StationID string   `parquet:"station_id"`
	Time      int64    `parquet:"time"`
	WDIRDeg   *int32   `parquet:"wdir_deg"`
	WSPDmS    *float64 `parquet:"wspd_ms"`
	GUSTmS    *float64 `parquet:"gust_ms"`
	PREShPa   *float64 `parquet:"pres_hpa"`
	ATMPC     *float64 `parquet:"atmp_c"`
	WTMPC     *float64 `parquet:"wtmp_c"`
	DEWPC     *float64 `parquet:"dewp_c"`
}

func buildSchema() *arrow.Schema {
	ts := &arrow.TimestampType{Unit: arrow.Second, TimeZone: "UTC"}
	return arrow.NewSchema([]arrow.Field{
		{Name: "station_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "time", Type: ts, Nullable: false},
		{Name: "wdir_deg", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "wspd_ms", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "gust_ms", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "pres_hpa", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "atmp_c", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "wtmp_c", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "dewp_c", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)
}

func appendOptF64(b *array.Float64Builder, p *float64) {
	if p == nil {
		b.AppendNull()
	} else {
		b.Append(*p)
	}
}

func rowsToRecord(mem memory.Allocator, schema *arrow.Schema, rows []MetRow) arrow.Record {
	ts := &arrow.TimestampType{Unit: arrow.Second, TimeZone: "UTC"}

	sb := array.NewStringBuilder(mem)
	tb := array.NewTimestampBuilder(mem, ts)
	wdirb := array.NewInt32Builder(mem)
	wspdb := array.NewFloat64Builder(mem)
	gustb := array.NewFloat64Builder(mem)
	presb := array.NewFloat64Builder(mem)
	atmpb := array.NewFloat64Builder(mem)
	wtmpb := array.NewFloat64Builder(mem)
	dewpb := array.NewFloat64Builder(mem)

	defer func() {
		sb.Release(); tb.Release(); wdirb.Release()
		wspdb.Release(); gustb.Release(); presb.Release()
		atmpb.Release(); wtmpb.Release(); dewpb.Release()
	}()

	for _, r := range rows {
		sb.Append(r.StationID)
		tb.Append(arrow.Timestamp(r.Time))
		if r.WDIRDeg == nil {
			wdirb.AppendNull()
		} else {
			wdirb.Append(*r.WDIRDeg)
		}
		appendOptF64(wspdb, r.WSPDmS)
		appendOptF64(gustb, r.GUSTmS)
		appendOptF64(presb, r.PREShPa)
		appendOptF64(atmpb, r.ATMPC)
		appendOptF64(wtmpb, r.WTMPC)
		appendOptF64(dewpb, r.DEWPC)
	}

	cols := []arrow.Array{
		sb.NewArray(), tb.NewArray(),
		wdirb.NewArray(), wspdb.NewArray(), gustb.NewArray(),
		presb.NewArray(), atmpb.NewArray(), wtmpb.NewArray(), dewpb.NewArray(),
	}
	rec := array.NewRecord(schema, cols, int64(len(rows)))
	for _, c := range cols {
		c.Release()
	}
	return rec
}

// readParquet reads all MetRows from a Parquet file using the generic reader.
func readParquet(path string) ([]MetRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := parquet.NewGenericReader[MetRow](f)
	defer r.Close()

	var all []MetRow
	buf := make([]MetRow, 1024)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			all = append(all, buf[:n]...)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return all, err
		}
	}
	return all, nil
}

func streamHandler(w http.ResponseWriter, _ *http.Request) {
	dataDir := getenv("DATA_DIR", "/data")
	mem := memory.NewGoAllocator()
	schema := buildSchema()

	w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")

	wr, err := ipc.NewWriter(w, ipc.WithSchema(schema))
	if err != nil {
		http.Error(w, "Arrow writer init: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer wr.Close()

	matches, _ := filepath.Glob(filepath.Join(dataDir, "*_latest.parquet"))
	if len(matches) == 0 {
		log.Printf("WARN no parquet files in %s", dataDir)
		return
	}

	for _, p := range matches {
		rows, err := readParquet(p)
		if err != nil {
			log.Printf("WARN readParquet %s: %v", p, err)
			continue
		}
		if len(rows) == 0 {
			continue
		}
		rec := rowsToRecord(mem, schema, rows)
		if err := wr.Write(rec); err != nil {
			log.Printf("ERROR ipc write %s: %v", p, err)
		}
		rec.Release()
		log.Printf("SENT  %s (%d rows)", p, len(rows))
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	port := getenv("ARROW_PORT", "8080")
	dataDir := getenv("DATA_DIR", "/data")
	log.Printf("Arrow source on :%s (GET /stream) | dataDir=%s", port, dataDir)

	http.HandleFunc("/stream", streamHandler)
	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	s := &http.Server{
		Addr:              ":" + port,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      60 * time.Second,
	}
	log.Fatal(s.ListenAndServe())
}
