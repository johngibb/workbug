This project illustrates a bug in gocraft/work when using EnqueueUniqueByKey.
You'll inevitably end up with leftover in-progress job queues that never clear,
leading to a Redis memory leak and eventually an out of memory situation.

To reproduce, start a local Redis instance (or update main.go to point to
another one elsewhere), and run:

```sh
go run main.go
```
