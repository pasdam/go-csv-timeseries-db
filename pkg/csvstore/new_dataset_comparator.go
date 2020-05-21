package csvstore

func newDatasetComparator(target uint64) func(interface{}) int {
	return func(value interface{}) int {
		d := value.(*dataPoint)
		return int(d.timestamp - target)
	}
}
