package csvstore

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pasdam/go-files-test/pkg/filestest"
	"github.com/pasdam/go-io-utilx/pkg/ioutilx"
	"github.com/pasdam/mockit/matchers/argument"
	"github.com/pasdam/mockit/mockit"
	"github.com/stretchr/testify/assert"
)

func TestNewStore(t *testing.T) {
	type args struct {
		dir      string
		interval uint64
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Should create instance with specified parameters",
			args: args{
				dir:      "some-folder",
				interval: 10,
			},
		},
		{
			name: "Should create instance with some other parameters",
			args: args{
				dir:      "some-other-folder",
				interval: 30,
			},
		},
		{
			name: "Should create instance with another folder",
			args: args{
				dir: "some-other-folder",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewStore(tt.args.dir, tt.args.interval)

			assert.Equal(t, tt.args.dir, got.dir)
			assert.Equal(t, tt.args.interval, got.index.interval)
		})
	}
}

func TestStore_LastPoint(t *testing.T) {
	type mocks struct {
		latestDatasetErr error
		readRecordsErr   error
	}
	tests := []struct {
		name          string
		mocks         mocks
		wantTimestamp uint64
		wantRecord    []string
	}{
		{
			name: "Should return error if latestDataset raises it",
			mocks: mocks{
				latestDatasetErr: errors.New("some-latest-dataset-error"),
			},
			wantTimestamp: 0,
			wantRecord:    nil,
		},
		{
			name: "Should return error if readRecords raises it",
			mocks: mocks{
				readRecordsErr: errors.New("some-read-records-error"),
			},
			wantTimestamp: 0,
			wantRecord:    nil,
		},
		{
			name:          "Should return the latest data point",
			wantTimestamp: 30,
			wantRecord:    []string{"some-value-at-30"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				dir: filepath.Join("testdata", "datasets"),
				index: index{
					interval: 10,
				},
			}
			wantErr := tt.mocks.latestDatasetErr
			if tt.mocks.latestDatasetErr != nil {
				mockit.MockFunc(t, latestDataset).With(s.dir).Return("", wantErr)
			}
			if tt.mocks.readRecordsErr != nil {
				wantErr = tt.mocks.readRecordsErr
				mockit.MockFunc(t, readRecords).With(filepath.Join("testdata", "datasets", "30_39.csv"), argument.Any).Return(wantErr)
			}

			gotTimestamp, gotRecord, err := s.LastPoint()

			assert.Equal(t, tt.wantTimestamp, gotTimestamp)
			assert.Equal(t, tt.wantRecord, gotRecord)
			assert.Equal(t, wantErr, err)
		})
	}
}

func TestStore_LoadPoints(t *testing.T) {
	type mocks struct {
		handlerErr error
	}
	type args struct {
		from uint64
		to   uint64
	}
	tests := []struct {
		name    string
		mocks   mocks
		args    args
		want    [][]interface{}
		wantErr error
	}{
		{
			name: "0 to 9",
			args: args{
				from: 0,
				to:   9,
			},
			want: [][]interface{}{
				{uint64(0), []string{"something-value-at-0"}},
				{uint64(8), []string{"something-value-at-8"}},
				{uint64(9), []string{"something-value-at-9"}},
			},
			wantErr: nil,
		},
		{
			name: "10 to 19",
			args: args{
				from: 10,
				to:   19,
			},
			want: [][]interface{}{
				{uint64(11), []string{"something-value-at-11"}},
				{uint64(13), []string{"something-value-at-13"}},
				{uint64(19), []string{"something-value-at-19"}},
			},
			wantErr: nil,
		},
		{
			name: "8 to 13",
			args: args{
				from: 8,
				to:   13,
			},
			want: [][]interface{}{
				{uint64(8), []string{"something-value-at-8"}},
				{uint64(9), []string{"something-value-at-9"}},
				{uint64(11), []string{"something-value-at-11"}},
				{uint64(13), []string{"something-value-at-13"}},
			},
			wantErr: nil,
		},
		{
			name: "Should return error if handler raises it",
			mocks: mocks{
				handlerErr: errors.New("some-handler-error"),
			},
			args: args{
				from: 8,
				to:   13,
			},
			want: [][]interface{}{
				{uint64(8), []string{"something-value-at-8"}},
			},
			wantErr: errors.New("some-handler-error"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				dir: filepath.Join("testdata", "datasets"),
				index: index{
					interval: 10,
				},
			}
			actualRecords := make(map[uint64][]string)
			handler := func(timestamp uint64, record []string) error {
				actualRecords[timestamp] = record
				return tt.mocks.handlerErr
			}

			err := s.LoadPoints(tt.args.from, tt.args.to, handler)

			assert.Equal(t, tt.wantErr, err)
			assert.Len(t, actualRecords, len(tt.want))
			for _, want := range tt.want {
				assert.Equal(t, want[1].([]string), actualRecords[want[0].(uint64)])
			}
		})
	}
}

func TestStore_StorePoints(t *testing.T) {
	type mocks struct {
		readErr        error
		writeErr       error
		datasetName    string
		datasetContent string
	}
	type file struct {
		path    string
		content string
	}
	type args struct {
		points TimeSeries
	}
	tests := []struct {
		name  string
		mocks mocks
		args  args
		want  []file
	}{
		{
			name: "Should return error if one occur while reading the datasets",
			mocks: mocks{
				readErr: errors.New("some-read-error"),
			},
			args: args{
				points: &mockTimeSeries{
					points: []*dataPoint{
						{timestamp: 19, record: []string{"some-value-at-19"}},
					},
				},
			},
		},
		{
			name: "Should return error if one occur while writing the datasets",
			mocks: mocks{
				writeErr: errors.New("some-write-error"),
			},
			args: args{
				points: &mockTimeSeries{
					points: []*dataPoint{
						{timestamp: 19, record: []string{"some-value-at-19"}},
					},
				},
			},
		},
		{
			name: "Should store points",
			args: args{
				points: &mockTimeSeries{
					points: []*dataPoint{
						{timestamp: 19, record: []string{"some-value-at-19"}},
						{timestamp: 15, record: []string{"some-value-at-15"}},
						{timestamp: 10, record: []string{"some-value-at-10"}},
						{timestamp: 9, record: []string{"some-value-at-9"}},
						{timestamp: 5, record: []string{"some-value-at-5"}},
						{timestamp: 0, record: []string{"some-value-at-0"}},
					},
				},
			},
			want: []file{
				{path: "0_9.csv", content: "0,some-value-at-0\n5,some-value-at-5\n9,some-value-at-9\n"},
				{path: "10_19.csv", content: "10,some-value-at-10\n15,some-value-at-15\n19,some-value-at-19\n"},
			},
		},
		{
			name: "Should merge points with existing dataset",
			mocks: mocks{
				datasetName:    "0_9.csv",
				datasetContent: "1,some-value-at-1\n3,some-value-at-3\n7,some-value-at-7\n",
			},
			args: args{
				points: &mockTimeSeries{
					points: []*dataPoint{
						{timestamp: 9, record: []string{"some-value-at-9"}},
						{timestamp: 5, record: []string{"some-value-at-5"}},
						{timestamp: 0, record: []string{"some-value-at-0"}},
					},
				},
			},
			want: []file{
				{path: "0_9.csv", content: "0,some-value-at-0\n1,some-value-at-1\n3,some-value-at-3\n5,some-value-at-5\n7,some-value-at-7\n9,some-value-at-9\n"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wantErr := tt.mocks.readErr
			if tt.mocks.readErr != nil {
				mockit.MockFunc(t, os.Stat).With(argument.Any).Return(nil, nil)
				mockit.MockFunc(t, readRecords).With(argument.Any, argument.Any).Return(wantErr)
			}
			if tt.mocks.writeErr != nil {
				wantErr = tt.mocks.writeErr
				mockit.MockFunc(t, writeDatasets).With(argument.Any).Return(wantErr)
			}
			dir := filestest.TempDir(t)
			if len(tt.mocks.datasetName) > 0 {
				reader := strings.NewReader(tt.mocks.datasetContent)
				ioutilx.ReaderToFile(reader, filepath.Join(dir, tt.mocks.datasetName))
			}

			s := &Store{
				dir:   dir,
				index: index{interval: 10},
			}

			err := s.StorePoints(tt.args.points)

			assert.Equal(t, wantErr, err)
			for _, file := range tt.want {
				filestest.FileExistsWithContent(t, filepath.Join(s.dir, file.path), file.content)
			}
		})
	}
}

func TestStore_merge(t *testing.T) {
	type fields struct {
		interval uint64
	}
	type args struct {
		datasets []*dataset
		points   *mockTimeSeries
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []*dataset
	}{
		{
			name: "Interval = 10",
			fields: fields{
				interval: 10,
			},
			args: args{
				datasets: []*dataset{
					{
						path: "0",
						points: dataPointList{
							&dataPoint{timestamp: 0, record: []string{"some-value-at-0"}},
						},
					},
					{
						path: "10",
						points: dataPointList{
							&dataPoint{timestamp: 10, record: []string{"some-value-at-10"}},
						},
					},
				},
				points: &mockTimeSeries{
					points: []*dataPoint{
						{timestamp: 0, record: []string{"some-other-value-at-0"}},
						{timestamp: 5, record: []string{"some-value-at-5"}},
						{timestamp: 9, record: []string{"some-value-at-9"}},
						{timestamp: 10, record: []string{"some-other-value-at-10"}},
						{timestamp: 15, record: []string{"some-value-at-15"}},
						{timestamp: 19, record: []string{"some-value-at-19"}},
					},
				},
			},
			want: []*dataset{
				{
					path: "0",
					points: dataPointList{
						&dataPoint{timestamp: 0, record: []string{"some-other-value-at-0"}},
						&dataPoint{timestamp: 5, record: []string{"some-value-at-5"}},
						&dataPoint{timestamp: 9, record: []string{"some-value-at-9"}},
					},
				},
				{
					path: "10",
					points: dataPointList{
						&dataPoint{timestamp: 10, record: []string{"some-other-value-at-10"}},
						&dataPoint{timestamp: 15, record: []string{"some-value-at-15"}},
						&dataPoint{timestamp: 19, record: []string{"some-value-at-19"}},
					},
				},
			},
		},
		{
			name: "Interval = 5",
			fields: fields{
				interval: 5,
			},
			args: args{
				datasets: []*dataset{
					{
						path: "0",
						points: dataPointList{
							&dataPoint{timestamp: 0, record: []string{"some-value-at-0"}},
						},
					},
					{
						path: "5",
						points: dataPointList{
							&dataPoint{timestamp: 5, record: []string{"some-value-at-5"}},
						},
					},
				},
				points: &mockTimeSeries{
					points: []*dataPoint{
						{timestamp: 0, record: []string{"some-other-value-at-0"}},
						{timestamp: 5, record: []string{"some-other-value-at-5"}},
						{timestamp: 9, record: []string{"some-value-at-9"}},
					},
				},
			},
			want: []*dataset{
				{
					path: "0",
					points: dataPointList{
						&dataPoint{timestamp: 0, record: []string{"some-other-value-at-0"}},
					},
				},
				{
					path: "5",
					points: dataPointList{
						&dataPoint{timestamp: 5, record: []string{"some-other-value-at-5"}},
						&dataPoint{timestamp: 9, record: []string{"some-value-at-9"}},
					},
				},
			},
		},
		{
			name: "Should add dataset if it does not exist",
			fields: fields{
				interval: 5,
			},
			args: args{
				datasets: []*dataset{},
				points: &mockTimeSeries{
					points: []*dataPoint{
						{timestamp: 0, record: []string{"some-other-value-at-0"}},
						{timestamp: 5, record: []string{"some-other-value-at-5"}},
						{timestamp: 9, record: []string{"some-value-at-9"}},
					},
				},
			},
			want: []*dataset{
				{
					path: "0_4.csv",
					points: dataPointList{
						&dataPoint{timestamp: 0, record: []string{"some-other-value-at-0"}},
					},
				},
				{
					path: "5_9.csv",
					points: dataPointList{
						&dataPoint{timestamp: 5, record: []string{"some-other-value-at-5"}},
						&dataPoint{timestamp: 9, record: []string{"some-value-at-9"}},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				index: index{
					interval: tt.fields.interval,
				},
			}
			datasetsMap := make(map[uint64]*dataset)
			for _, ds := range tt.args.datasets {
				datasetsMap[ds.points[0].timestamp] = ds
			}

			s.merge(datasetsMap, tt.args.points)

			for _, ds := range tt.want {
				assert.Equal(t, ds, datasetsMap[ds.points[0].timestamp])
			}
		})
	}
}

func TestStore_readDatasets(t *testing.T) {
	type fields struct {
		interval uint64
	}
	type args struct {
		from uint64
		to   uint64
	}
	type wantDs struct {
		ds  dataset
		key uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []wantDs
		wantErr error
	}{
		{
			name: "Should load one dataset",
			fields: fields{
				interval: 10,
			},
			args: args{
				from: 0,
				to:   9,
			},
			want: []wantDs{
				{
					ds: dataset{
						path: filepath.Join("testdata", "datasets", "0_9.csv"),
						points: []*dataPoint{
							{0, []string{"something-value-at-0"}},
							{8, []string{"something-value-at-8"}},
							{9, []string{"something-value-at-9"}},
						},
					},
					key: 0,
				},
			},
		},
		{
			name: "Should load one dataset with no sample for the first timestamp",
			fields: fields{
				interval: 10,
			},
			args: args{
				from: 13,
				to:   17,
			},
			want: []wantDs{
				{
					ds: dataset{
						path: filepath.Join("testdata", "datasets", "10_19.csv"),
						points: []*dataPoint{
							{11, []string{"something-value-at-11"}},
							{13, []string{"something-value-at-13"}},
							{19, []string{"something-value-at-19"}},
						},
					},
					key: 10,
				},
			},
		},
		{
			name: "Should load two dataset",
			fields: fields{
				interval: 10,
			},
			args: args{
				from: 0,
				to:   19,
			},
			want: []wantDs{
				{
					ds: dataset{
						path: filepath.Join("testdata", "datasets", "0_9.csv"),
						points: []*dataPoint{
							{0, []string{"something-value-at-0"}},
							{8, []string{"something-value-at-8"}},
							{9, []string{"something-value-at-9"}},
						},
					},
					key: 0,
				},
				{
					ds: dataset{
						path: filepath.Join("testdata", "datasets", "10_19.csv"),
						points: []*dataPoint{
							{11, []string{"something-value-at-11"}},
							{13, []string{"something-value-at-13"}},
							{19, []string{"something-value-at-19"}},
						},
					},
					key: 10,
				},
			},
		},
		{
			name: "Should return empty map if no dataset exists",
			fields: fields{
				interval: 10,
			},
			args: args{
				from: 40,
				to:   49,
			},
			want: make([]wantDs, 0),
		},
		{
			name: "Should return error if readRecords raises it",
			fields: fields{
				interval: 10,
			},
			args: args{
				from: 0,
				to:   29,
			},
			want:    nil,
			wantErr: errors.New("strconv.ParseUint: parsing \"invalid-record\": invalid syntax"),
		},
		{
			name: "Should not raise error if the interval is greater than maxInt64",
			fields: fields{
				interval: math.MaxInt64 + 1,
			},
			args: args{
				from: 0,
				to:   math.MaxInt64,
			},
			want: []wantDs{
				{
					ds: dataset{
						path:   filepath.Join("testdata", "datasets", "0_9223372036854775807.csv"),
						points: []*dataPoint{},
					},
					key: 0,
				},
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				dir: filepath.Join("testdata", "datasets"),
				index: index{
					interval: tt.fields.interval,
				},
			}

			got, err := s.readDatasets(tt.args.from, tt.args.to)

			if tt.wantErr != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.wantErr.Error(), err.Error())
			} else {
				assert.Nil(t, err)
			}
			assert.Equal(t, len(tt.want), len(got))
			for _, d := range tt.want {
				assert.Equal(t, &d.ds, got[d.key])
			}
		})
	}
}
