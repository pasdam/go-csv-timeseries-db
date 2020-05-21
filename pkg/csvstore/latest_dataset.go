package csvstore

import (
	"io/ioutil"
)

func latestDataset(dir string) (string, error) {
	infos, err := ioutil.ReadDir(dir)
	if err != nil {
		return "", err
	}

	latestName := ""
	var latestTo uint64
	for _, info := range infos {
		currentName := info.Name()
		_, currentTo, err := parseDatasetName(currentName)
		if err != nil {
			return "", err
		}

		if currentTo > latestTo {
			latestName = currentName
			latestTo = currentTo
		}
	}

	return latestName, nil
}
