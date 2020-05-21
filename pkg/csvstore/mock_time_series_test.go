package csvstore

type mockTimeSeries struct {
	points []*dataPoint
}

func (s *mockTimeSeries) Len() int {
	return len(s.points)
}

func (s *mockTimeSeries) Less(i, j int) bool {
	return s.points[i].timestamp < s.points[j].timestamp
}

func (s *mockTimeSeries) Swap(i, j int) {
	temp := s.points[i]
	s.points[i] = s.points[j]
	s.points[j] = temp
}

func (s *mockTimeSeries) CsvAtIndex(index int) (record []string) {
	return s.points[index].record
}

func (s *mockTimeSeries) TimestampAtIndex(index int) (timestamp uint64) {
	return s.points[index].timestamp
}
