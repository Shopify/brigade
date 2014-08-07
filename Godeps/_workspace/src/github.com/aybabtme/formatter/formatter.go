// Package formatter makes it easy to create and compose logrus.Formatters.
package formatter

import (
	"github.com/Sirupsen/logrus"
)

var _ logrus.Formatter = Func((&logrus.JSONFormatter{}).Format)

// Func implements the logrus.Formatter interface. Any function implementing
// the signature `func(*logrus.Entry) ([]byte, error)` can be wrapped by
// Func and then be used as a proper logrus.Formatter.
type Func func(*logrus.Entry) ([]byte, error)

// Format delegates to the func implementation that this Func wraps.
func (f Func) Format(l *logrus.Entry) ([]byte, error) { return f(l) }

// Before will create a logrus.Formatter that invokes `beforeFunc` before sending
// a logrus.Entry to the wrapped formatter.
func Before(beforeFunc func(*logrus.Entry) *logrus.Entry, formatter logrus.Formatter) logrus.Formatter {
	return Func(func(e *logrus.Entry) ([]byte, error) {
		return formatter.Format(beforeFunc(e))
	})
}
