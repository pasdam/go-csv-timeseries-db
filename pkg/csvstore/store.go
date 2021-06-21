package csvstore

import (
	"os"
	"path/filepath"
	"sort"
)

// Store represent the db, and allows to load datapoints from CSV files
type Store struct {
	dir   string
	index index
}

// NewStore creates a new instance of a Store that saves/loads CSV to/from the
// specified folder, the whole dataset will be split into subset of interval
// size
func NewStore(dir string, interval uint64) *Store {
	return &Store{
		dir: dir,
		index: index{
			interval: interval,
		},
	}
}

// LastPoint returns the last data point in the store
func (s *Store) LastPoint() (timestamp uint64, record []string, err error) {
	name, err := latestDataset(s.dir)
	if err != nil {
		return 0, nil, err
	}

	var points []*dataPoint
	handler := newTimestampHandler(newRecordsCollector(&points))

	err = readRecords(s.path(name), handler)
	if err != nil || len(points) == 0 {
		return 0, nil, err
	}

	last := points[len(points)-1]

	return last.timestamp, last.record, nil
}

// LoadPoints return all the datapoints between from and to.
// The parameter pointHandler is called for each record, and will receive the
// its timestamp and the remaining columns as string.
func (s *Store) LoadPoints(from uint64, to uint64, pointHandler func(uint64, []string) error) error {
	handler := newTimestampHandler(newFilterRecordsHandler(from, to, pointHandler))

	for _, name := range s.index.findDatasets(from, to) {
		err := readRecords(s.path(name), handler)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
	}

	return nil
}

// StorePoints persists the data points in the timeserie in the store.
// Note it will sort the series before storing it.
func (s *Store) StorePoints(points TimeSeries) error {
	sort.Sort(points)

	from := points.TimestampAtIndex(0)
	to := points.TimestampAtIndex(points.Len() - 1)

	datasets, err := s.readDatasets(from, to)
	if err != nil {
		return err
	}

	s.merge(datasets, points)
	err = writeDatasets(datasets)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) merge(datasets map[uint64]*dataset, points TimeSeries) {
	for i := 0; i < points.Len(); i++ {
		timestamp := points.TimestampAtIndex(i)
		from := (timestamp / s.index.interval) * s.index.interval

		d := datasets[from]
		if d == nil {
			d = &dataset{
				path: s.path(datasetName(from, from+s.index.interval-1)),
			}
			datasets[from] = d
		}

		insert(timestamp, points.CsvAtIndex(i), &d.points)
	}
}

func (s *Store) path(datasetName string) string {
	return filepath.Join(s.dir, datasetName)
}

func (s *Store) readDatasets(from uint64, to uint64) (map[uint64]*dataset, error) {
	datasetNames := s.index.findDatasets(from, to)
	datasets := make(map[uint64]*dataset)

	maxSize := s.index.interval
	if maxSize > 10000 {
		maxSize = 10000
	}

	for i := 0; i < len(datasetNames); i++ {
		from, _, _ := parseDatasetName(datasetNames[i])

		path := s.path(datasetNames[i])

		_, err := os.Stat(path)
		if err != nil {
			continue
		}

		points := make([]*dataPoint, 0, maxSize)
		d := &dataset{
			path: path,
		}

		handler := newTimestampHandler(newRecordsCollector(&points))

		err = readRecords(d.path, handler)
		if err != nil {
			return nil, err
		}

		d.points = points
		datasets[from] = d
	}

	return datasets, nil
}
