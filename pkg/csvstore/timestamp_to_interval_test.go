package csvstore

import "testing"

func Test_timestampToInterval(t *testing.T) {
	type args struct {
		timestamp uint64
		interval  uint64
	}
	tests := []struct {
		name     string
		args     args
		wantFrom uint64
		wantTo   uint64
	}{
		{
			name: "From 0 to 9",
			args: args{
				timestamp: 1,
				interval:  10,
			},
			wantFrom: 0,
			wantTo:   9,
		},
		{
			name: "From 23 to 45",
			args: args{
				timestamp: 24,
				interval:  23,
			},
			wantFrom: 23,
			wantTo:   45,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFrom, gotTo := timestampToInterval(tt.args.timestamp, tt.args.interval)
			if gotFrom != tt.wantFrom {
				t.Errorf("timestampToInterval() gotFrom = %v, want %v", gotFrom, tt.wantFrom)
			}
			if gotTo != tt.wantTo {
				t.Errorf("timestampToInterval() gotTo = %v, want %v", gotTo, tt.wantTo)
			}
		})
	}
}
