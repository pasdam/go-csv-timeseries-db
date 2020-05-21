package csvstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_insert(t *testing.T) {
	type args struct {
		timestamp uint64
		record    []string
		points    dataPointList
	}
	tests := []struct {
		name string
		args args
		want dataPointList
	}{
		{
			name: "Should replace existing point",
			args: args{
				timestamp: 1,
				record:    []string{"some-other-value-at-1"},
				points: []*dataPoint{
					{timestamp: 0, record: []string{"some-value-at-0"}},
					{timestamp: 1, record: []string{"some-value-at-1"}},
					{timestamp: 2, record: []string{"some-value-at-2"}},
				},
			},
			want: []*dataPoint{
				{timestamp: 0, record: []string{"some-value-at-0"}},
				{timestamp: 1, record: []string{"some-other-value-at-1"}},
				{timestamp: 2, record: []string{"some-value-at-2"}},
			},
		},
		{
			name: "Should insert as first element if array is empty",
			args: args{
				timestamp: 0,
				record:    []string{"some-value-at-0"},
				points:    []*dataPoint{},
			},
			want: []*dataPoint{
				{timestamp: 0, record: []string{"some-value-at-0"}},
			},
		},
		{
			name: "Should insert as first element, when array has already other greater elements",
			args: args{
				timestamp: 0,
				record:    []string{"some-value-at-0"},
				points: []*dataPoint{
					{timestamp: 1, record: []string{"some-value-at-1"}},
					{timestamp: 2, record: []string{"some-value-at-2"}},
					{timestamp: 3, record: []string{"some-value-at-3"}},
				},
			},
			want: []*dataPoint{
				{timestamp: 0, record: []string{"some-value-at-0"}},
				{timestamp: 1, record: []string{"some-value-at-1"}},
				{timestamp: 2, record: []string{"some-value-at-2"}},
				{timestamp: 3, record: []string{"some-value-at-3"}},
			},
		},
		{
			name: "Should insert as second element",
			args: args{
				timestamp: 1,
				record:    []string{"some-value-at-1"},
				points: []*dataPoint{
					{timestamp: 0, record: []string{"some-value-at-0"}},
					{timestamp: 2, record: []string{"some-value-at-2"}},
					{timestamp: 3, record: []string{"some-value-at-3"}},
				},
			},
			want: []*dataPoint{
				{timestamp: 0, record: []string{"some-value-at-0"}},
				{timestamp: 1, record: []string{"some-value-at-1"}},
				{timestamp: 2, record: []string{"some-value-at-2"}},
				{timestamp: 3, record: []string{"some-value-at-3"}},
			},
		},
		{
			name: "Should insert as second element",
			args: args{
				timestamp: 3,
				record:    []string{"some-value-at-3"},
				points: []*dataPoint{
					{timestamp: 0, record: []string{"some-value-at-0"}},
					{timestamp: 1, record: []string{"some-value-at-1"}},
					{timestamp: 2, record: []string{"some-value-at-2"}},
				},
			},
			want: []*dataPoint{
				{timestamp: 0, record: []string{"some-value-at-0"}},
				{timestamp: 1, record: []string{"some-value-at-1"}},
				{timestamp: 2, record: []string{"some-value-at-2"}},
				{timestamp: 3, record: []string{"some-value-at-3"}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			insert(tt.args.timestamp, tt.args.record, &tt.args.points)

			assert.Equal(t, len(tt.want), len(tt.args.points))
			assert.Equal(t, tt.want, tt.args.points)
		})
	}
}
