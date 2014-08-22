package humanize

import (
	"testing"
)

func TestOrders(t *testing.T) {
	testTable := []struct {
		name string
		got  string
		want string
	}{
		{"orders(0)", Orders(0.0), "0"},
		{"orders(1)", Orders(1.0), "1"},
		{"orders(0.1)", Orders(0.1), "0.1"},
		{"orders(0.1)", Orders(0.11), "0.1"},
		{"orders(0.1)", Orders(0.111), "0.1"},
		{"orders(0.2)", Orders(0.151), "0.2"},
		{"orders(11.0m)", Orders(0.011), "11.0m"},
		{"orders(1.0m)", Orders(0.001), "1.0m"},
		{"orders(51.0m)", Orders(0.051), "51.0m"},
		{"orders(51.0µ)", Orders(0.000051), "51.0µ"},
		{"orders(5.1µ)", Orders(0.0000051), "5.1µ"},
		{"orders(0.5µ)", Orders(0.00000051), "0.5µ"},
		{"orders(51.0n)", Orders(0.000000051), "51.0n"},
		{"orders(5.0p)", Orders(0.000000000005), "5.0p"},
		{"orders(0.5p)", Orders(0.0000000000005), "0.5p"},
		{"orders(50.0f)", Orders(0.00000000000005), "50.0f"},
		{"orders(5.0f)", Orders(0.000000000000005), "5.0f"},
		{"orders(803)", Orders(803.0), "803"},
		{"orders(803)", Orders(803.4), "803.4"},
		{"orders(999)", Orders(999.0), "999"},
		{"orders(1000)", Orders(1000.0), "1.0K"},
		{"orders(1M)", Orders(1000 * 1000.0), "1.0M"},
		{"orders(1G)", Orders(giga * 1.0), "1.0G"},
		{"orders(1T)", Orders(tera * 1.0), "1.0T"},
		{"orders(1P)", Orders(peta * 1.0), "1.0P"},
		{"orders(1E)", Orders(ecto * 1.0), "1.0E"},
		{"orders(1000E)", Orders(ecto * 1000.0), "1000.0E"},
		{"orders(5.5G)", Orders(5.5 * giga * 1.0), "5.5G"},

		{"orders(-1)", Orders(-1.0), "-1"},
		{"orders(-0.1)", Orders(-0.1), "-0.1"},
		{"orders(-0.1)", Orders(-0.11), "-0.1"},
		{"orders(-0.1)", Orders(-0.111), "-0.1"},
		{"orders(-0.2)", Orders(-0.151), "-0.2"},
		{"orders(-11.0m)", Orders(-0.011), "-11.0m"},
		{"orders(-1.0m)", Orders(-0.001), "-1.0m"},
		{"orders(-51.0m)", Orders(-0.051), "-51.0m"},
		{"orders(-51.0µ)", Orders(-0.000051), "-51.0µ"},
		{"orders(-5.1µ)", Orders(-0.0000051), "-5.1µ"},
		{"orders(-0.5µ)", Orders(-0.00000051), "-0.5µ"},
		{"orders(-51.0n)", Orders(-0.000000051), "-51.0n"},
		{"orders(-5.0p)", Orders(-0.000000000005), "-5.0p"},
		{"orders(-0.5p)", Orders(-0.0000000000005), "-0.5p"},
		{"orders(-50.0f)", Orders(-0.00000000000005), "-50.0f"},
		{"orders(-5.0f)", Orders(-0.000000000000005), "-5.0f"},
		{"orders(-5.0f)", Orders(-0.000000000000000005), "-0.0f"},
		{"orders(-803)", Orders(-803.0), "-803"},
		{"orders(-803)", Orders(-803.4), "-803.4"},
		{"orders(-999)", Orders(-999.0), "-999"},
		{"orders(-1000)", Orders(-1000.0), "-1.0K"},
		{"orders(-1M)", Orders(1000 * -1000.0), "-1.0M"},
		{"orders(-1G)", Orders(giga * -1.0), "-1.0G"},
		{"orders(-1T)", Orders(tera * -1.0), "-1.0T"},
		{"orders(-1P)", Orders(peta * -1.0), "-1.0P"},
		{"orders(-1E)", Orders(ecto * -1.0), "-1.0E"},
		{"orders(-5.5G)", Orders(5.5 * giga * -1.0), "-5.5G"},
	}

	for _, test := range testTable {
		if test.got != test.want {
			t.Errorf("%s: want '%s' got '%s'", test.name, test.want, test.got)
		}
	}
}
