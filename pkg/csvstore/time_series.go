package csvstore

import "sort"

// TimeSeries contains the method to retrieve elements of a time series
type TimeSeries interface {
	sort.Interface

	// CsvAtIndex returns the csv representation (excluding the timestamp) of the
	// data point at the specified index
	CsvAtIndex(index int) (record []string)

	// TimestampAtIndex returns the timestamp of the data point at the specified
	// index
	TimestampAtIndex(index int) (timestamp uint64)
}
