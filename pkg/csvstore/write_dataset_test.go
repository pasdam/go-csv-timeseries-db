package csvstore

import (
	"encoding/csv"
	"errors"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"bou.ke/monkey"

	"github.com/pasdam/go-files-test/pkg/filestest"
	"github.com/pasdam/mockit/mockit"
	"github.com/stretchr/testify/assert"
)

func Test_writeDataset(t *testing.T) {
	type mocks struct {
		fileContent string
		createErr   error
		writeErr    error
	}
	type args struct {
		ds *dataset
	}
	tests := []struct {
		name            string
		mocks           mocks
		args            args
		expectedContent string
	}{
		{
			name: "Should return error if os.Create raises it",
			mocks: mocks{
				createErr: errors.New("some-create-error"),
			},
			args: args{
				ds: &dataset{
					path: filestest.TempFile(t, "some-create-error-path"),
				},
			},
		},
		{
			name: "Should return error if writer.Write raises it",
			mocks: mocks{
				writeErr: errors.New("some-write-error"),
			},
			args: args{
				ds: &dataset{
					path: filestest.TempFile(t, "some-write-error-path"),
					points: dataPointList{
						{timestamp: 0, record: []string{"some-write-error-value-at-0"}},
					},
				},
			},
		},
		{
			name: "Should create file if it does not exist",
			args: args{
				ds: &dataset{
					path: filestest.TempFile(t, "some-new-file-path"),
					points: dataPointList{
						{timestamp: 0, record: []string{"some-new-file-value-at-0"}},
					},
				},
			},
			expectedContent: "0,some-new-file-value-at-0\n",
		},
		{
			name: "Should overwrite file if it already exists",
			mocks: mocks{
				fileContent: "some-existing-file-content",
			},
			args: args{
				ds: &dataset{
					path: filestest.TempFile(t, "some-existing-file-path"),
					points: dataPointList{
						{timestamp: 1, record: []string{"some-existing-file-value-at-1"}},
					},
				},
			},
			expectedContent: "1,some-existing-file-value-at-1\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wantErr := tt.mocks.createErr
			if tt.mocks.createErr != nil {
				mockit.MockFunc(t, os.Create).With(tt.args.ds.path).Return(nil, wantErr)
			}
			var guard *monkey.PatchGuard
			if tt.mocks.writeErr != nil {
				wantErr = tt.mocks.writeErr
				var writer *csv.Writer
				guard = monkey.PatchInstanceMethod(reflect.TypeOf(writer), "Write", func(w *csv.Writer, record []string) error {
					return wantErr
				})
			}
			if wantErr == nil {
				if len(tt.mocks.fileContent) > 0 {
					err := ioutil.WriteFile(tt.args.ds.path, []byte(tt.mocks.fileContent), os.ModeAppend)
					assert.Nil(t, err)
				}
			}

			err := writeDataset(tt.args.ds)

			if guard != nil {
				guard.Unpatch()
			}

			assert.Equal(t, wantErr, err)
			filestest.FileExistsWithContent(t, tt.args.ds.path, tt.expectedContent)
		})
	}
}
