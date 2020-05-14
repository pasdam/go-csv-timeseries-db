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
				record: []string{"9", "some-less-than-from-value"},
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
				record: []string{"1001", "some-greater-than-to-value"},
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
				record:    []string{"20", "some-equal-to-from-value"},
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
				record:    []string{"500", "some-equal-to-to-value"},
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
				record:    []string{"3000", "some-in-between-value"},
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
				record:    []string{"3000", "some-in-between-value"},
				err:       errors.New("some-handler-error"),
			},
			shouldCallNext: true,
			wantErr:        errors.New("some-handler-error"),
		},
		{
			name: "Should not call next and return error if timestamp is invalid",
			factoryArgs: factoryArgs{
				from: 200,
				to:   500,
			},
			handlerArgs: handlerArgs{
				record: []string{"invalid-timestamp", "some-invalid-timestamp-value"},
			},
			shouldCallNext: false,
			wantErr:        errors.New("strconv.ParseUint: parsing \"invalid-timestamp\": invalid syntax"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			handler := func(timestamp uint64, record []string) error {
				called = true
				assert.Equal(t, tt.handlerArgs.timestamp, timestamp)
				assert.Equal(t, tt.handlerArgs.record[1:], record)
				return tt.handlerArgs.err
			}

			err := newFilterRecordsHandler(tt.factoryArgs.from, tt.factoryArgs.to, handler)(tt.handlerArgs.record)

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
