package csvstore

type dataPointList []*dataPoint

func (d dataPointList) Length() int {
	return len(d)
}

func (d dataPointList) ElementAt(index int) interface{} {
	return d[index]
}
