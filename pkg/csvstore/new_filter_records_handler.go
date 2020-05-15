package csvstore

func newFilterRecordsHandler(from uint64, to uint64, handler func(uint64, []string) error) func(uint64, []string) error {
	return func(timestamp uint64, record []string) error {
		if from <= timestamp && timestamp <= to {
			return handler(timestamp, record)
		}

		return nil
	}
}
