package csvstore

type index struct {
	interval uint64
}

func (i *index) findDataset(timestamp uint64) string {
	from, to := timestampToInterval(timestamp, i.interval)
	return datasetName(from, to)
}

func (i *index) findDatasets(from uint64, to uint64) []string {
	startFrom, _ := timestampToInterval(from, i.interval)
	_, endTo := timestampToInterval(to, i.interval)

	count := (endTo - startFrom + 1) / i.interval

	result := make([]string, count)

	currentFrom := startFrom
	for j := uint64(0); j < count; j++ {
		currentTo := currentFrom + i.interval
		result[j] = datasetName(currentFrom, currentTo-1)
		currentFrom = currentTo
	}

	return result
}
