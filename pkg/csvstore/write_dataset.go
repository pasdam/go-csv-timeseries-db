package csvstore

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"
)

func writeDataset(ds *dataset) error {
	parent := filepath.Dir(ds.path)
	_, err := os.Stat(parent)
	if err != nil {
		if os.IsNotExist(err) {
			// create
			err = os.MkdirAll(parent, os.ModePerm)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	file, err := os.Create(ds.path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for i := 0; i < ds.points.Length(); i++ {
		record := make([]string, 0, len(ds.points[i].record)+1)
		record = append(record, strconv.FormatUint(ds.points[i].timestamp, 10))
		record = append(record, ds.points[i].record...)

		err = writer.Write(record)
		if err != nil {
			return err
		}
	}

	return nil
}
