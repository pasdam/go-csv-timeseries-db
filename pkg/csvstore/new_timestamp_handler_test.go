package csvstore

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_newTimestampHandler(t *testing.T) {
	type handler struct {
		timestamp uint64
		record    []string
		err       error
	}
	tests := []struct {
		name           string
		handler        handler
		shouldCallNext bool
		wantErr        error
	}{
		{
			name: "Should call next handler if timestamp is valid",
			handler: handler{
				timestamp: 10,
				record:    []string{"10", "some-other-column-1"},
			},
			shouldCallNext: true,
		},
		{
			name: "Should not call next handler and return an error if timestamp is invalid",
			handler: handler{
				record: []string{"invalid-value", "some-other-column-2"},
			},
			shouldCallNext: false,
			wantErr:        errors.New("strconv.ParseUint: parsing \"invalid-value\": invalid syntax"),
		},
		{
			name: "Should return error if next handler raises it",
			handler: handler{
				timestamp: 100,
				record:    []string{"100", "some-other-column-3"},
				err:       errors.New("some-next-handler-error"),
			},
			shouldCallNext: true,
			wantErr:        errors.New("some-next-handler-error"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			handler := func(timestamp uint64, record []string) error {
				called = true
				assert.Equal(t, tt.handler.timestamp, timestamp)
				assert.Equal(t, tt.handler.record[1:], record)
				return tt.handler.err
			}

			err := newTimestampHandler(handler)(tt.handler.record)

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
