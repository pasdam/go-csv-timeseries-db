package csvstore

import (
	"fmt"
)

func datasetName(from uint64, to uint64) string {
	return fmt.Sprintf("%d_%d.csv", from, to)
}
