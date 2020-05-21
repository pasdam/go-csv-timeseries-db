package csvstore

import (
	"errors"
	"testing"

	"github.com/pasdam/go-files-test/pkg/filestest"
	"github.com/pasdam/mockit/mockit"
	"github.com/stretchr/testify/assert"
)

func Test_writeDatasets(t *testing.T) {
	type mocks struct {
		writeErr error
	}
	type args struct {
		datasets []*dataset
	}
	tests := []struct {
		name             string
		mocks            mocks
		args             args
		expectedContents []string
	}{
		{
			name: "Should return error if writeDataset raises it",
			mocks: mocks{
				writeErr: errors.New("some-write-error"),
			},
			args: args{
				datasets: []*dataset{
					{
						path: "some-write-error-path",
						points: dataPointList{
							&dataPoint{
								timestamp: 0,
								record:    []string{"some-dataset-0-value-at-0"},
							},
						},
					},
				},
			},
		},
		{
			name: "Should store datasets",
			args: args{
				datasets: []*dataset{
					{
						path: filestest.TempFile(t, "0.csv"),
						points: dataPointList{
							&dataPoint{
								timestamp: 0,
								record:    []string{"some-dataset-0-value-at-0"},
							},
						},
					},
					{
						path: filestest.TempFile(t, "1.csv"),
						points: dataPointList{
							&dataPoint{
								timestamp: 1,
								record:    []string{"some-dataset-1-value-at-1"},
							},
						},
					},
					{
						path: filestest.TempFile(t, "2.csv"),
						points: dataPointList{
							&dataPoint{
								timestamp: 2,
								record:    []string{"some-dataset-2-value-at-2"},
							},
						},
					},
				},
			},
			expectedContents: []string{
				"0,some-dataset-0-value-at-0\n",
				"1,some-dataset-1-value-at-1\n",
				"2,some-dataset-2-value-at-2\n",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wantErr := tt.mocks.writeErr
			if tt.mocks.writeErr != nil {
				mockit.MockFunc(t, writeDataset).With(tt.args.datasets[0]).Return(wantErr)
			}
			datasetsMap := make(map[uint64]*dataset)
			for _, ds := range tt.args.datasets {
				datasetsMap[ds.points[0].timestamp] = ds
			}

			err := writeDatasets(datasetsMap)

			assert.Equal(t, wantErr, err)
			for i := 0; i < len(tt.args.datasets); i++ {
				if wantErr != nil {
					assert.NoFileExists(t, tt.args.datasets[i].path)
				} else {
					filestest.FileExistsWithContent(t, tt.args.datasets[i].path, tt.expectedContents[i])
				}
			}
		})
	}
}
