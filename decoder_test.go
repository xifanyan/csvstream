package csvstream

import (
	"os"
	"reflect"
	"testing"
)

type timeseries struct {
	TimeStamp        string  `gorm:"primary_key" csv:"timestamp"`
	Function         uint8   `gorm:"primary_key;auto_increment:false"`
	Symbol           string  `gorm:"primary_key"`
	Open             float64 `csv:"open"`
	High             float64 `csv:"high"`
	Low              float64 `csv:"low"`
	Close            float64 `csv:"close"`
	AdjustedClose    float64 `csv:"adjusted_close"`
	Volume           int     `csv:"volume"`
	Dividend         float64 `csv:"dividend_amount"`
	SplitCoefficient float64 `csv:"split_coefficient"`
}

func TestGetCSVHeader(t *testing.T) {

	csvfile := "testdata/withheader.csv"
	f, _ := os.Open(csvfile)

	dec, _ := NewDecoder(f, &timeseries{})
	_ = dec.setHeader()

	expected := []string{"timestamp", "open", "high", "low", "close", "adjusted_close",
		"volume", "dividend_amount", "split_coefficient"}
	if !reflect.DeepEqual(dec.header, expected) {
		t.Errorf("%v\n", dec.header)
	}

}
