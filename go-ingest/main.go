package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	parquet "github.com/parquet-go/parquet-go"
)

// MetRow is the schema for each observation row written to Parquet.
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

const ndbcBase = "https://www.ndbc.noaa.gov/data/realtime2"

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// atoiP parses an integer, returning nil for sentinel values (99, 999, 9999).
func atoiP(s string) *int32 {
	if s == "" {
		return nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v == 99 || v == 999 || v == 9999 {
		return nil
	}
	x := int32(v)
	return &x
}

// atofP parses a float, returning nil for sentinel values.
func atofP(s string) *float64 {
	if s == "" {
		return nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || v == 99 || v == 999 || v == 9999 {
		return nil
	}
	return &v
}

func get(cols []string, idx map[string]int, key string) string {
	if i, ok := idx[strings.ToUpper(key)]; ok && i >= 0 && i < len(cols) {
		return cols[i]
	}
	return ""
}

// parseNdbcStdMet parses NDBC standard meteorological text data.
// It dynamically finds the header line and maps columns by name.
func parseNdbcStdMet(station string, body []byte, maxRows int) ([]MetRow, error) {
	r := bufio.NewReader(bytes.NewReader(body))
	var header []string
	var data [][]string

	for {
		lineBytes, _, err := r.ReadLine()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		line := string(lineBytes)

		if strings.HasPrefix(line, "#") {
			trim := strings.TrimSpace(strings.TrimPrefix(line, "#"))
			// First comment line starting with YY or YYYY is the header.
			if strings.HasPrefix(trim, "YY") || strings.HasPrefix(trim, "YYYY") {
				header = strings.Fields(trim)
			}
			// Second comment line (units) is skipped automatically.
			continue
		}
		if strings.TrimSpace(line) == "" {
			continue
		}
		cols := strings.Fields(line)
		if len(cols) >= 5 {
			data = append(data, cols)
		}
	}

	// Fallback header if not found in file.
	if header == nil {
		header = []string{
			"YYYY", "MM", "DD", "hh", "mm",
			"WDIR", "WSPD", "GST", "WVHT", "DPD",
			"APD", "MWD", "PRES", "PTDY", "ATMP",
			"WTMP", "DEWP", "VIS", "TIDE",
		}
	}

	idx := make(map[string]int, len(header))
	for i, h := range header {
		idx[strings.ToUpper(h)] = i
	}

	if maxRows > 0 && len(data) > maxRows {
		data = data[:maxRows]
	}

	out := make([]MetRow, 0, len(data))
	for _, cols := range data {
		// Determine year column name (YYYY or YY).
		yy := get(cols, idx, "YYYY")
		if yy == "" {
			yy = get(cols, idx, "YY")
		}
		mm := get(cols, idx, "MM")
		dd := get(cols, idx, "DD")
		hh := get(cols, idx, "HH")
		if hh == "" {
			hh = get(cols, idx, "hh")
		}
		mn := get(cols, idx, "mm")

		year, _ := strconv.Atoi(yy)
		if len(yy) == 2 {
			year += 2000
		}
		month, _ := strconv.Atoi(mm)
		day, _ := strconv.Atoi(dd)
		hour, _ := strconv.Atoi(hh)
		minute, _ := strconv.Atoi(mn)

		t := time.Date(year, time.Month(month), day, hour, minute, 0, 0, time.UTC)

		out = append(out, MetRow{
			StationID: strings.ToUpper(station),
			Time:      t.Unix(),
			WDIRDeg:   atoiP(get(cols, idx, "WDIR")),
			WSPDmS:    atofP(get(cols, idx, "WSPD")),
			GUSTmS:    atofP(get(cols, idx, "GST")),
			PREShPa:   atofP(get(cols, idx, "PRES")),
			ATMPC:     atofP(get(cols, idx, "ATMP")),
			WTMPC:     atofP(get(cols, idx, "WTMP")),
			DEWPC:     atofP(get(cols, idx, "DEWP")),
		})
	}

	return out, nil
}

func fetchStation(ctx context.Context, station string) ([]MetRow, error) {
	u := fmt.Sprintf("%s/%s.txt", ndbcBase, strings.ToUpper(station))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", station, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch %s: HTTP %d", station, resp.StatusCode)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return parseNdbcStdMet(station, b, 48)
}

// writeParquet atomically writes rows to path via a .tmp intermediate file.
func writeParquet(path string, rows []MetRow) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	w := parquet.NewGenericWriter[MetRow](f)
	if _, err := w.Write(rows); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := w.Close(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func runOnce(ctx context.Context, stations []string, dataDir string) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		log.Printf("ERROR mkdir %s: %v", dataDir, err)
		return
	}
	for _, s := range stations {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		rows, err := fetchStation(ctx, s)
		if err != nil {
			log.Printf("WARN  %s: %v", s, err)
			continue
		}
		if len(rows) == 0 {
			log.Printf("INFO  %s: no rows parsed", s)
			continue
		}
		out := filepath.Join(dataDir, strings.ToUpper(s)+"_latest.parquet")
		if err := writeParquet(out, rows); err != nil {
			log.Printf("ERROR %s: write parquet: %v", s, err)
			continue
		}
		log.Printf("WROTE %s (%d rows)", out, len(rows))
	}
}

func main() {
	stationsCSV := getenv("STATIONS", "SANF1,SMKF1,LONF1,VAKF1,KYWF1")
	stations := strings.Split(stationsCSV, ",")
	dataDir := getenv("DATA_DIR", "/data")
	minsStr := getenv("REFRESH_MINUTES", "60")
	mins, _ := strconv.Atoi(minsStr)

	log.Printf("Starting go-ingest | stations=%s refresh=%dmin dataDir=%s",
		stationsCSV, mins, dataDir)

	ctx := context.Background()
	for {
		runOnce(ctx, stations, dataDir)
		if mins <= 0 {
			log.Println("One-shot mode complete, exiting.")
			break
		}
		log.Printf("Sleeping %d minutes until next fetch.", mins)
		time.Sleep(time.Duration(mins) * time.Minute)
	}
}
