package csvstore

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_latestDataset(t *testing.T) {
	type args struct {
		dir string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr error
	}{
		{
			name: "Should return latest dataset in existing folder",
			args: args{
				dir: filepath.Join("testdata", "datasets"),
			},
			want:    "30_39.csv",
			wantErr: nil,
		},
		{
			name: "Should return error if folder does not exist",
			args: args{
				dir: "some-not-existing-folder",
			},
			wantErr: errors.New("open some-not-existing-folder: no such file or directory"),
		},
		{
			name: "Should return error if parseDatasetName raises it",
			args: args{
				dir: filepath.Join("testdata", "csv"),
			},
			wantErr: errors.New("Wrong file name format: invalid.csv"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := latestDataset(tt.args.dir)

			if tt.wantErr != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.wantErr.Error(), err.Error())
			} else {
				assert.Nil(t, err)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}
