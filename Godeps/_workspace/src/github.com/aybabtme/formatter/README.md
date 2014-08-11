formatter
======

Create ad-hocs `logrus.Formatter`.

```go
id := uuid.New()
// long tasks will have each field tagged with a task id
beforeLog := func(e *logrus.Entry) *logrus.Entry {
    return e.WithField("task id", id)
}
myFormatter := formatter.Before(beforeLog, &logrus.TextFormatter)
logrus.SetFormatter(myFormatter)
```
