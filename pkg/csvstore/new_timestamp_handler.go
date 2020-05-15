package csvstore

import (
	"strconv"
)

func newTimestampHandler(handler func(uint64, []string) error) func([]string) error {
	return func(record []string) error {
		timestamp, err := strconv.ParseUint(record[0], 10, 64)
		if err != nil {
			return err
		}

		return handler(timestamp, record[1:])
	}
}
