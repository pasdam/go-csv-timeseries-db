package csvstore

import (
	"errors"
	"path/filepath"
	"strconv"
	"strings"
)

func parseDatasetName(name string) (from uint64, to uint64, err error) {
	fileTimestamps := strings.Split(strings.TrimSuffix(name, filepath.Ext(name)), "_")

	if len(fileTimestamps) != 2 {
		return 0, 0, errors.New("Wrong file name format: " + name)
	}

	from, err = strconv.ParseUint(fileTimestamps[0], 0, 64)
	if err != nil {
		return 0, 0, err
	}

	to, err = strconv.ParseUint(fileTimestamps[1], 0, 64)
	if err != nil {
		return 0, 0, err
	}

	return from, to, nil
}
