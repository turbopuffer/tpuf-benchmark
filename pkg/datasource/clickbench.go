package datasource

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"iter"
	"sync"
	"text/template"
	"time"

	"github.com/parquet-go/parquet-go"
)

const (
	clickBenchPartitionCount = 100
	clickBenchURLFmt         = "https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_%d.parquet"
	clickBenchCacheKeyFmt    = "clickbench/hits_%d.parquet"
	clickBenchRowBufSize     = 1024
)

// ClickBench returns a datasource that yields ClickBench rows from the 100
// partitioned parquet files (99,997,497 rows), with all 105 hits columns.
func ClickBench(_ context.Context, cfg Config) Source {
	cfg.ParseConcurrency = max(1, cfg.ParseConcurrency)
	return &clickBenchSource{dd: newDownloader(cfg)}
}

type clickBenchSource struct {
	dd *downloader

	once sync.Once
	next func() (hitRow, error, bool)
}

var _ Source = (*clickBenchSource)(nil)

// hitRow is a ClickBench hits row with its timestamp and date columns converted
// to RFC3339 for turbopuffer datetime fields. The string fields shadow the raw
// parquet values of the same name.
type hitRow struct {
	hitParquetRow
	EventTime       string
	EventDate       string
	ClientEventTime string
	LocalEventTime  string
}

// hitParquetRow is every column of ClickBench hits. EventTime, ClientEventTime
// and LocalEventTime are Unix epoch seconds (INT64); EventDate is days since
// 1970-01-01.
type hitParquetRow struct {
	WatchID               int64  `parquet:"WatchID"`
	JavaEnable            int32  `parquet:"JavaEnable"`
	Title                 string `parquet:"Title"`
	GoodEvent             int32  `parquet:"GoodEvent"`
	EventTime             int64  `parquet:"EventTime"`
	EventDate             int32  `parquet:"EventDate"`
	CounterID             int32  `parquet:"CounterID"`
	ClientIP              int32  `parquet:"ClientIP"`
	RegionID              int32  `parquet:"RegionID"`
	UserID                int64  `parquet:"UserID"`
	CounterClass          int32  `parquet:"CounterClass"`
	OS                    int32  `parquet:"OS"`
	UserAgent             int32  `parquet:"UserAgent"`
	URL                   string `parquet:"URL"`
	Referer               string `parquet:"Referer"`
	IsRefresh             int32  `parquet:"IsRefresh"`
	RefererCategoryID     int32  `parquet:"RefererCategoryID"`
	RefererRegionID       int32  `parquet:"RefererRegionID"`
	URLCategoryID         int32  `parquet:"URLCategoryID"`
	URLRegionID           int32  `parquet:"URLRegionID"`
	ResolutionWidth       int32  `parquet:"ResolutionWidth"`
	ResolutionHeight      int32  `parquet:"ResolutionHeight"`
	ResolutionDepth       int32  `parquet:"ResolutionDepth"`
	FlashMajor            int32  `parquet:"FlashMajor"`
	FlashMinor            int32  `parquet:"FlashMinor"`
	FlashMinor2           string `parquet:"FlashMinor2"`
	NetMajor              int32  `parquet:"NetMajor"`
	NetMinor              int32  `parquet:"NetMinor"`
	UserAgentMajor        int32  `parquet:"UserAgentMajor"`
	UserAgentMinor        string `parquet:"UserAgentMinor"`
	CookieEnable          int32  `parquet:"CookieEnable"`
	JavascriptEnable      int32  `parquet:"JavascriptEnable"`
	IsMobile              int32  `parquet:"IsMobile"`
	MobilePhone           int32  `parquet:"MobilePhone"`
	MobilePhoneModel      string `parquet:"MobilePhoneModel"`
	Params                string `parquet:"Params"`
	IPNetworkID           int32  `parquet:"IPNetworkID"`
	TraficSourceID        int32  `parquet:"TraficSourceID"`
	SearchEngineID        int32  `parquet:"SearchEngineID"`
	SearchPhrase          string `parquet:"SearchPhrase"`
	AdvEngineID           int32  `parquet:"AdvEngineID"`
	IsArtifical           int32  `parquet:"IsArtifical"`
	WindowClientWidth     int32  `parquet:"WindowClientWidth"`
	WindowClientHeight    int32  `parquet:"WindowClientHeight"`
	ClientTimeZone        int32  `parquet:"ClientTimeZone"`
	ClientEventTime       int64  `parquet:"ClientEventTime"`
	SilverlightVersion1   int32  `parquet:"SilverlightVersion1"`
	SilverlightVersion2   int32  `parquet:"SilverlightVersion2"`
	SilverlightVersion3   int32  `parquet:"SilverlightVersion3"`
	SilverlightVersion4   int32  `parquet:"SilverlightVersion4"`
	PageCharset           string `parquet:"PageCharset"`
	CodeVersion           int32  `parquet:"CodeVersion"`
	IsLink                int32  `parquet:"IsLink"`
	IsDownload            int32  `parquet:"IsDownload"`
	IsNotBounce           int32  `parquet:"IsNotBounce"`
	FUniqID               int64  `parquet:"FUniqID"`
	OriginalURL           string `parquet:"OriginalURL"`
	HID                   int32  `parquet:"HID"`
	IsOldCounter          int32  `parquet:"IsOldCounter"`
	IsEvent               int32  `parquet:"IsEvent"`
	IsParameter           int32  `parquet:"IsParameter"`
	DontCountHits         int32  `parquet:"DontCountHits"`
	WithHash              int32  `parquet:"WithHash"`
	HitColor              string `parquet:"HitColor"`
	LocalEventTime        int64  `parquet:"LocalEventTime"`
	Age                   int32  `parquet:"Age"`
	Sex                   int32  `parquet:"Sex"`
	Income                int32  `parquet:"Income"`
	Interests             int32  `parquet:"Interests"`
	Robotness             int32  `parquet:"Robotness"`
	RemoteIP              int32  `parquet:"RemoteIP"`
	WindowName            int32  `parquet:"WindowName"`
	OpenerName            int32  `parquet:"OpenerName"`
	HistoryLength         int32  `parquet:"HistoryLength"`
	BrowserLanguage       string `parquet:"BrowserLanguage"`
	BrowserCountry        string `parquet:"BrowserCountry"`
	SocialNetwork         string `parquet:"SocialNetwork"`
	SocialAction          string `parquet:"SocialAction"`
	HTTPError             int32  `parquet:"HTTPError"`
	SendTiming            int32  `parquet:"SendTiming"`
	DNSTiming             int32  `parquet:"DNSTiming"`
	ConnectTiming         int32  `parquet:"ConnectTiming"`
	ResponseStartTiming   int32  `parquet:"ResponseStartTiming"`
	ResponseEndTiming     int32  `parquet:"ResponseEndTiming"`
	FetchTiming           int32  `parquet:"FetchTiming"`
	SocialSourceNetworkID int32  `parquet:"SocialSourceNetworkID"`
	SocialSourcePage      string `parquet:"SocialSourcePage"`
	ParamPrice            int64  `parquet:"ParamPrice"`
	ParamOrderID          string `parquet:"ParamOrderID"`
	ParamCurrency         string `parquet:"ParamCurrency"`
	ParamCurrencyID       int32  `parquet:"ParamCurrencyID"`
	OpenstatServiceName   string `parquet:"OpenstatServiceName"`
	OpenstatCampaignID    string `parquet:"OpenstatCampaignID"`
	OpenstatAdID          string `parquet:"OpenstatAdID"`
	OpenstatSourceID      string `parquet:"OpenstatSourceID"`
	UTMSource             string `parquet:"UTMSource"`
	UTMMedium             string `parquet:"UTMMedium"`
	UTMCampaign           string `parquet:"UTMCampaign"`
	UTMContent            string `parquet:"UTMContent"`
	UTMTerm               string `parquet:"UTMTerm"`
	FromTag               string `parquet:"FromTag"`
	HasGCLID              int32  `parquet:"HasGCLID"`
	RefererHash           int64  `parquet:"RefererHash"`
	URLHash               int64  `parquet:"URLHash"`
	CLID                  int32  `parquet:"CLID"`
}

func (r hitParquetRow) asHitRow() hitRow {
	return hitRow{
		hitParquetRow:   r,
		EventTime:       unixRFC3339(r.EventTime),
		EventDate:       time.Unix(int64(r.EventDate)*86400, 0).UTC().Format("2006-01-02T00:00:00Z"),
		ClientEventTime: unixRFC3339(r.ClientEventTime),
		LocalEventTime:  unixRFC3339(r.LocalEventTime),
	}
}

func unixRFC3339(sec int64) string {
	return time.Unix(sec, 0).UTC().Format(time.RFC3339)
}

func (s *clickBenchSource) FuncMap(ctx context.Context) template.FuncMap {
	s.once.Do(func() {
		s.next = lazyPull2(func() iter.Seq2[hitRow, error] {
			return parsingAndDownloadingIterator(ctx, s.dd, clickBenchURLs(), parseClickBench)
		})
	})
	return template.FuncMap{
		"hit": func() hitRow {
			row, err, ok := s.next()
			if !ok {
				panic("ClickBench hits source exhausted")
			} else if err != nil {
				panic(err)
			}
			return row
		},
	}
}

func clickBenchURLs() iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		for i := range clickBenchPartitionCount {
			if !yield(fmt.Sprintf(clickBenchCacheKeyFmt, i), fmt.Sprintf(clickBenchURLFmt, i)) {
				return
			}
		}
	}
}

func parseClickBench(mmapped *MemoryMappedFile) (iter.Seq[hitRow], error) {
	f, err := parquet.OpenFile(bytes.NewReader(mmapped.Data), int64(len(mmapped.Data)))
	if err != nil {
		return nil, fmt.Errorf("failed to open ClickBench parquet file: %w", err)
	}
	return func(yield func(hitRow) bool) {
		buf := make([]hitParquetRow, clickBenchRowBufSize)
		for _, rg := range f.RowGroups() {
			r := parquet.NewGenericRowGroupReader[hitParquetRow](rg)
			for {
				n, err := r.Read(buf)
				for i := range n {
					if !yield(buf[i].asHitRow()) {
						r.Close()
						return
					}
				}
				if err == io.EOF {
					break
				} else if err != nil {
					r.Close()
					panic(fmt.Errorf("reading ClickBench parquet rows: %w", err))
				}
			}
			if err := r.Close(); err != nil {
				panic(fmt.Errorf("closing ClickBench parquet reader: %w", err))
			}
		}
	}, nil
}
