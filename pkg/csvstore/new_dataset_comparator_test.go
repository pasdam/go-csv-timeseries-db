package csvstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_newDatasetComparator(t *testing.T) {
	type args struct {
		target uint64
		value  *dataPoint
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "Should return a value greater then 0 if target is less than value",
			args: args{
				target: 10,
				value: &dataPoint{
					timestamp: 12,
				},
			},
			want: 2,
		},
		{
			name: "Should return 0 if target is equal to value",
			args: args{
				target: 20,
				value: &dataPoint{
					timestamp: 20,
				},
			},
			want: 0,
		},
		{
			name: "Should return a value less then 0 if target is greater than value",
			args: args{
				target: 30,
				value: &dataPoint{
					timestamp: 20,
				},
			},
			want: -10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newDatasetComparator(tt.args.target)(tt.args.value)

			assert.Equal(t, tt.want, got)
		})
	}
}
