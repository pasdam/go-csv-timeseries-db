package csvstore

func newRecordsCollector(points *[]*dataPoint) func(uint64, []string) error {
	return func(timestmap uint64, record []string) error {
		p := &dataPoint{
			timestamp: timestmap,
			record:    record,
		}

		*points = append(*points, p)

		return nil
	}
}
