package csvstore

import (
	"strconv"
)

func newFilterRecordsHandler(from uint64, to uint64, handler func(uint64, []string) error) func([]string) error {
	return func(record []string) error {
		timestamp, err := strconv.ParseUint(record[0], 10, 64)
		if err != nil {
			return err
		}

		if from <= timestamp && timestamp <= to {
			return handler(timestamp, record[1:])
		}

		return nil
	}
}
