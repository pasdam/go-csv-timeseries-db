package csvstore

import (
	"errors"
	"testing"
)

func Test_parseDatasetName(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name     string
		args     args
		wantFrom uint64
		wantTo   uint64
		wantErr  error
	}{
		{
			name: "Success",
			args: args{
				name: "1234_5678.csv",
			},
			wantFrom: 1234,
			wantTo:   5678,
			wantErr:  nil,
		},
		{
			name: "InvalidFormat",
			args: args{
				name: "1234_5678_invalid.csv",
			},
			wantFrom: 0,
			wantTo:   0,
			wantErr:  errors.New("Wrong file name format: 1234_5678_invalid.csv"),
		},
		{
			name: "InvalidFrom",
			args: args{
				name: "invalid-from_5678.csv",
			},
			wantFrom: 0,
			wantTo:   0,
			wantErr:  errors.New("strconv.ParseUint: parsing \"invalid-from\": invalid syntax"),
		},
		{
			name: "InvalidFrom",
			args: args{
				name: "1234_invalid-to.csv",
			},
			wantFrom: 0,
			wantTo:   0,
			wantErr:  errors.New("strconv.ParseUint: parsing \"invalid-to\": invalid syntax"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFrom, gotTo, err := parseDatasetName(tt.args.name)
			if err != nil && err.Error() != tt.wantErr.Error() {
				t.Errorf("parseDatasetName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotFrom != tt.wantFrom {
				t.Errorf("parseDatasetName() gotFrom = %v, want %v", gotFrom, tt.wantFrom)
			}
			if gotTo != tt.wantTo {
				t.Errorf("parseDatasetName() gotTo = %v, want %v", gotTo, tt.wantTo)
			}
		})
	}
}
