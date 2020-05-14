package csvstore

import (
	"testing"
)

func Test_datasetName(t *testing.T) {
	type args struct {
		from uint64
		to   uint64
	}
	tests := []struct {
		args args
		want string
	}{
		{
			args: args{
				from: 0,
				to:   0,
			},
			want: "0_0.csv",
		},
		{
			args: args{
				from: 1234,
				to:   5678,
			},
			want: "1234_5678.csv",
		},
		{
			args: args{
				from: 234,
				to:   789,
			},
			want: "234_789.csv",
		},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := datasetName(tt.args.from, tt.args.to); got != tt.want {
				t.Errorf("datasetName() = %v, want %v", got, tt.want)
			}
		})
	}
}
