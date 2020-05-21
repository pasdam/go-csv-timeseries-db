package csvstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_newRecordsCollector(t *testing.T) {
	type args struct {
		points    []*dataPoint
		timestamp uint64
		record    []string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Should append to empty points list",
			args: args{
				points:    []*dataPoint{},
				timestamp: 123,
				record:    []string{"some-empty-list-value"},
			},
		},
		{
			name: "Should append to non empty points list",
			args: args{
				points: []*dataPoint{
					{
						timestamp: 789,
						record:    []string{"some-existing-record-value"},
					},
				},
				timestamp: 456,
				record:    []string{"some-not-empty-list-value"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			points := make([]*dataPoint, 0, len(tt.args.points))
			points = append(points, tt.args.points...)

			err := newRecordsCollector(&points)(tt.args.timestamp, tt.args.record)

			assert.Nil(t, err)
			assert.Equal(t, tt.args.points, points[:len(points)-1])
			assert.Equal(t, tt.args.timestamp, points[len(points)-1].timestamp)
			assert.Equal(t, tt.args.record, points[len(points)-1].record)
		})
	}
}
