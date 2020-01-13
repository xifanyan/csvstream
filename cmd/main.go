package main

import (
	"fmt"
	"os"

	"github.com/xifanyan/csvstream"
)

type timeseries struct {
	TimeStamp     string `csv:"timestamp"`
	Symbol        string
	Open          float64 `csv:"open"`
	High          float64 `csv:"high"`
	Low           float64 `csv:"low"`
	Close         float64 `csv:"close"`
	AdjustedClose float64 `csv:"adjusted_close"`
	Volume        int     `csv:"volume"`
}

func main() {

	f, _ := os.Open("testdata/withheader.csv")
	dec, _ := csvstream.NewDecoder(f, &timeseries{})

	c, _ := dec.Unmarshal()

	for row := range c {
		fmt.Printf("%v\n", row.(timeseries))
	}
}
