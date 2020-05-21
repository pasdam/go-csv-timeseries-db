package csvstore

func writeDatasets(datasets map[uint64]*dataset) error {
	for _, ds := range datasets {
		err := writeDataset(ds)
		if err != nil {
			return err
		}
	}
	return nil
}
