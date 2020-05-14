package csvstore

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
				{uint64(10), []string{"something-value-at-10"}},
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
				{uint64(10), []string{"something-value-at-10"}},
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
				dir: "testdata",
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
