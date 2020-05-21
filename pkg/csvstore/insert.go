package csvstore

import (
	"github.com/pasdam/go-search/pkg/search"
)

func insert(timestamp uint64, record []string, points *dataPointList) {
	point := &dataPoint{
		timestamp: timestamp,
		record:    record,
	}

	index, found := search.BinarySearch(newDatasetComparator(timestamp), *points)
	if found {
		// replace
		(*points)[index] = point

	} else {
		// insert
		pointsSlice := *points
		last := len(pointsSlice) - 1
		if last >= 0 {
			pointsSlice = append(pointsSlice, pointsSlice[last]) // extend array
			if index <= last {
				copy(pointsSlice[index+1:], pointsSlice[index:last]) // shift elements
			}
		} else {
			pointsSlice = append(pointsSlice, nil) // extend array
		}
		pointsSlice[index] = point // insert element
		*points = pointsSlice
	}
}
