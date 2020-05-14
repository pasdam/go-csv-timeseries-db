package csvstore

func timestampToInterval(timestamp uint64, interval uint64) (from uint64, to uint64) {
	from = timestamp / interval * interval
	to = from + interval - 1
	return from, to
}
