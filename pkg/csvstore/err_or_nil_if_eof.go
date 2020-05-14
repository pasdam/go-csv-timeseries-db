package csvstore

import (
	"io"
)

func errOrNilIfEOF(err error) error {
	if err == io.EOF {
		return nil
	}
	return err
}
