package csvstore

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_newFilterRecordsHandler(t *testing.T) {
	type factoryArgs struct {
		from uint64
		to   uint64
	}
	type handlerArgs struct {
		timestamp uint64
		record    []string
		err       error
	}
	tests := []struct {
		name           string
		factoryArgs    factoryArgs
		handlerArgs    handlerArgs
		shouldCallNext bool
		wantErr        error
	}{
		{
			name: "Should not call next if timestamp is less than from",
			factoryArgs: factoryArgs{
				from: 10,
				to:   100,
			},
			handlerArgs: handlerArgs{
				timestamp: 9,
				record:    []string{"some-less-than-from--col-0-value", "some-less-than-from--col-1-value"},
			},
			shouldCallNext: false,
		},
		{
			name: "Should not call next if timestamp is greater than to",
			factoryArgs: factoryArgs{
				from: 100,
				to:   1000,
			},
			handlerArgs: handlerArgs{
				record: []string{"some-greater-than-to--col-0-value", "some-greater-than-to--col-1-value"},
			},
			shouldCallNext: false,
		},
		{
			name: "Should call next if timestamp is equal to from",
			factoryArgs: factoryArgs{
				from: 20,
				to:   50,
			},
			handlerArgs: handlerArgs{
				timestamp: 20,
				record:    []string{"some-equal-to-from--col-0-value", "some-equal-to-from--col-1-value"},
			},
			shouldCallNext: true,
		},
		{
			name: "Should call next if timestamp is equal to to",
			factoryArgs: factoryArgs{
				from: 200,
				to:   500,
			},
			handlerArgs: handlerArgs{
				timestamp: 500,
				record:    []string{"some-equal-to-to--col-0-value", "some-equal-to-to--col-1-value"},
			},
			shouldCallNext: true,
		},
		{
			name: "Should call next if timestamp is between from and to",
			factoryArgs: factoryArgs{
				from: 2000,
				to:   5000,
			},
			handlerArgs: handlerArgs{
				timestamp: 3000,
				record:    []string{"some-in-between--col-0-value", "some-in-between--col-1-value"},
			},
			shouldCallNext: true,
		},
		{
			name: "Should return error if handler raises it",
			factoryArgs: factoryArgs{
				from: 2000,
				to:   5000,
			},
			handlerArgs: handlerArgs{
				timestamp: 3000,
				record:    []string{"some-in-between-col-0-value", "some-in-between-col-1-value"},
				err:       errors.New("some-handler-error"),
			},
			shouldCallNext: true,
			wantErr:        errors.New("some-handler-error"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			handler := func(timestamp uint64, record []string) error {
				called = true
				assert.Equal(t, tt.handlerArgs.timestamp, timestamp)
				assert.Equal(t, tt.handlerArgs.record, record)
				return tt.handlerArgs.err
			}

			err := newFilterRecordsHandler(tt.factoryArgs.from, tt.factoryArgs.to, handler)(tt.handlerArgs.timestamp, tt.handlerArgs.record)

			assert.Equal(t, tt.shouldCallNext, called)
			if tt.wantErr != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.wantErr.Error(), err.Error())
			} else {
				assert.Nil(t, err)
			}
		})
	}
}
