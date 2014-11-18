package humanize

import (
	"fmt"
	"math"
)

// SI Sizes.
const (
	femto = pico / 1000.0
	pico  = nano / 1000.0
	nano  = micro / 1000.0
	micro = milli / 1000.0
	milli = unit / 1000.0
	unit  = 1.0
	kilo  = unit * 1000.0
	mega  = kilo * 1000.0
	giga  = mega * 1000.0
	tera  = giga * 1000.0
	peta  = tera * 1000.0
	ecto  = peta * 1000.0
)

var ordersSizeTable = map[float64]string{
	femto: "f",
	pico:  "p",
	nano:  "n",
	micro: "µ",
	milli: "m",
	unit:  "",
	kilo:  "K",
	mega:  "M",
	giga:  "G",
	tera:  "T",
	peta:  "P",
	ecto:  "E",
}

var ordersLess = []string{
	0: "",
	1: "m",
	2: "µ",
	3: "n",
	4: "p",
	5: "f",
}

// order = 1.0
// value = 100.0, 0.1
// value 803 should give 803
func fitsOrder(order float64, value float64) bool {
	if order == unit && value > 100.0 && value < 1000.0 {
		return true
	}

	if value/10.0 > order {
		return false
	}

	if value*100.0 < order {
		return false
	}
	return true
}

func Orders(s float64) string {
	if s == 0.0 {
		return "0"
	}

	value := math.Abs(s)
	if value < 1.0 {
		value *= 10.0
		var order int
		for order = 0; value < 1.0 && order < len(ordersLess)-1; value *= 1000.0 {
			order++
			s *= 1000.0
		}

		return fmt.Sprintf("%.1f%s", s, ordersLess[order])
	}

	for order, suffix := range ordersSizeTable {
		if fitsOrder(order, value) {
			if order == unit && value-math.Floor(value) < 0.0001 {
				return fmt.Sprintf("%d", int(s))
			}
			return fmt.Sprintf("%3.1f%s", s/order, suffix)
		}
	}
	return fmt.Sprintf("%3.1f%s", s/ecto, ordersSizeTable[ecto])
}
