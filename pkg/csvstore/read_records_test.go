package csvstore

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/pasdam/mockit/mockit"
	"github.com/stretchr/testify/assert"
)

func Test_readRecords(t *testing.T) {
	type mocks struct {
		openErr    error
		handlerErr error
	}
	type args struct {
		path string
	}
	tests := []struct {
		name    string
		mocks   mocks
		args    args
		want    [][]string
		wantErr error
	}{
		{
			name: "Should return error if os.Open raises it",
			mocks: mocks{
				openErr: errors.New("some-open-error"),
			},
			args: args{
				path: "",
			},
			wantErr: errors.New("some-open-error"),
		},
		{
			name: "Should return error if CSV is invalid",
			args: args{
				path: filepath.Join("testdata", "csv", "invalid.csv"),
			},
			want: [][]string{
				{"some", "invalid"},
			},
			wantErr: errors.New("record on line 2: wrong number of fields"),
		},
		{
			name: "Should return error if handler raises it",
			mocks: mocks{
				handlerErr: errors.New("some-handler-error"),
			},
			args: args{
				path: filepath.Join("testdata", "csv", "valid.csv"),
			},
			want: [][]string{
				{"some", "valid"},
			},
			wantErr: errors.New("some-handler-error"),
		},
		{
			name: "Should read records from valid CSV",
			args: args{
				path: filepath.Join("testdata", "csv", "valid.csv"),
			},
			want: [][]string{
				{"some", "valid"},
				{"csv", ""},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.mocks.openErr != nil {
				mockit.MockFunc(t, os.Open).With(tt.args.path).Return(nil, tt.mocks.openErr)
			}

			var actualRecords [][]string
			handler := func(record []string) error {
				actualRecords = append(actualRecords, record)
				return tt.mocks.handlerErr
			}

			err := readRecords(tt.args.path, handler)

			if tt.wantErr != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.wantErr.Error(), err.Error())
			} else {
				assert.Nil(t, err)
			}
			// assert.True(t, reflect.DeepEqual(tt.want, actualRecords))
			assert.Equal(t, tt.want, actualRecords)
		})
	}
}
