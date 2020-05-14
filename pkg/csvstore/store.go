package csvstore

import (
	"path/filepath"
)

// Store represent the db, and allows to load datapoints from CSV files
type Store struct {
	dir   string
	index index
}

// NewStore creates a new instance of a Store that saves/loads CSV to/from the
// specified folder
func NewStore(dir string) *Store {
	return &Store{
		dir: dir,
		index: index{
			interval: 100,
		},
	}
}

// LoadPoints return all the datapoints between from and to.
// The parameter pointHandler is called for each record, and will receive the
// its timestamp and the remaining columns as string.
func (s *Store) LoadPoints(from uint64, to uint64, pointHandler func(uint64, []string) error) error {
	datasetNames := s.index.findDatasets(from, to)

	handler := newFilterRecordsHandler(from, to, pointHandler)

	for _, name := range datasetNames {
		err := readRecords(filepath.Join(s.dir, name), handler)
		if err != nil {
			return err
		}
	}

	return nil
}
