package csvstore

import (
	"encoding/csv"
	"os"
	"strconv"
)

func writeDataset(ds *dataset) error {
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
