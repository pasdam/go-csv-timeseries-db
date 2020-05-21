package csvstore

import (
	"reflect"
	"testing"
)

func Test_dataPointList_Length(t *testing.T) {
	tests := []struct {
		name string
		d    dataPointList
		want int
	}{
		{
			name: "Should return correct length (3)",
			d:    make(dataPointList, 3),
			want: 3,
		},
		{
			name: "Should return correct length (5)",
			d:    make(dataPointList, 5),
			want: 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.d.Length(); got != tt.want {
				t.Errorf("dataPointList.Length() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_dataPointList_ElementAt(t *testing.T) {
	d := make(dataPointList, 2)
	d[0] = &dataPoint{
		timestamp: 0,
	}
	d[1] = &dataPoint{
		timestamp: 1,
	}
	type args struct {
		index int
	}
	tests := []struct {
		name string
		args args
		want interface{}
	}{
		{
			name: "Should the element at the index 0",
			args: args{
				index: 0,
			},
			want: d[0],
		},
		{
			name: "Should the element at the index 1",
			args: args{
				index: 1,
			},
			want: d[1],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := d.ElementAt(tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("dataPointList.ElementAt() = %v, want %v", got, tt.want)
			}
		})
	}
}
