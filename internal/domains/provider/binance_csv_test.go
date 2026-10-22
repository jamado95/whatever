package provider

import (
	"strings"
	"testing"
)

/*
TestParseCsvRecordToCandle covers the timestamp unit: archives up to 2024 carry ms and pass through;
archives from 2025-01 carry µs and are scaled to ms.
*/
func TestParseCsvRecordToCandle(t *testing.T) {
	tests := []struct {
		name          string
		row           string
		open, closeTs int64
	}{
		{"ms record unchanged",
			"1704067200000,42274.27,42390.63,42255.49,42390.63,3.60936,1704067499999,152835.49,123,3.34,141690.42,0",
			1704067200000, 1704067499999},
		{"µs record scaled to ms",
			"1735689600000000,93390.63,93512.00,93352.00,93466.76,6.184,1735689899999999,577888.68,364,3.93,368066.55,0",
			1735689600000, 1735689899999},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, err := parseCsvRecordToCandle(strings.Split(tc.row, ","))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if c.OpenTs != tc.open || c.CloseTs != tc.closeTs {
				t.Fatalf("OpenTs, CloseTs = %d, %d; want %d, %d", c.OpenTs, c.CloseTs, tc.open, tc.closeTs)
			}
		})
	}
}
