(Bucket) Brigade is a command line application written in Go.  It uses the excellent https://wiki.ubuntu.com/goamz library for S3 access.

It's got one goal: fast S3 bucket<->bucket data sync.  Will do differential updates so it's useful as a task.

Supports S3 and S3-like cloud storages (like Google Cloud Storage in interoperability mode, for example).

