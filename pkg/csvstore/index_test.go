package csvstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_index_findDataset(t *testing.T) {
	type fields struct {
		interval uint64
	}
	type args struct {
		timestamp uint64
	}
	tests := []struct {
		fields fields
		args   args
		want   string
	}{
		{
			fields: fields{
				interval: 10,
			},
			args: args{
				timestamp: 5,
			},
			want: "0_9.csv",
		},
		{
			fields: fields{
				interval: 10,
			},
			args: args{
				timestamp: 15,
			},
			want: "10_19.csv",
		},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			i := &index{
				interval: tt.fields.interval,
			}
			if got := i.findDataset(tt.args.timestamp); got != tt.want {
				t.Errorf("index.findDataset() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_index_findDatasets(t *testing.T) {
	type fields struct {
		interval uint64
	}
	type args struct {
		from uint64
		to   uint64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []string
	}{
		{
			name: "Within first dataset",
			fields: fields{
				interval: 10,
			},
			args: args{
				from: 1,
				to:   3,
			},
			want: []string{"0_9.csv"},
		},
		{
			name: "Within second dataset",
			fields: fields{
				interval: 10,
			},
			args: args{
				from: 11,
				to:   13,
			},
			want: []string{"10_19.csv"},
		},
		{
			name: "Across 2 dataset",
			fields: fields{
				interval: 10,
			},
			args: args{
				from: 1,
				to:   10,
			},
			want: []string{"0_9.csv", "10_19.csv"},
		},
		{
			name: "Across 3 dataset",
			fields: fields{
				interval: 5,
			},
			args: args{
				from: 4,
				to:   10,
			},
			want: []string{"0_4.csv", "5_9.csv", "10_14.csv"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &index{
				interval: tt.fields.interval,
			}

			got := i.findDatasets(tt.args.from, tt.args.to)

			assert.Equal(t, tt.want, got)
		})
	}
}
