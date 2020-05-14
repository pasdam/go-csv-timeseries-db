package csvstore

import (
	"encoding/csv"
	"os"
)

func readRecords(path string, recordHandler func([]string) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// create CSV reader from file
	reader := csv.NewReader(file)
	for {
		record, err := reader.Read()
		if err != nil {
			return errOrNilIfEOF(err)
		}

		err = recordHandler(record)
		if err != nil {
			return errOrNilIfEOF(err)
		}
	}
}
