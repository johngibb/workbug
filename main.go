package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gocraft/work"
	"github.com/gomodule/redigo/redis"
)

type Context struct{}

const namespace = "workbug2"

func main() {
	var (
		finish = make(chan struct{})
		done   = make(chan struct{})
		pool   = &redis.Pool{
			MaxActive: 5,
			MaxIdle:   5,
			Wait:      true,
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", ":6379")
			},
		}
	)

	conn := pool.Get()
	defer conn.Close()
	// conn.Do("flushdb")

	// Make sure redis is clean for this namespace.
	existingKeys, err := redis.Strings(conn.Do("KEYS", namespace+"*"))
	must(err)
	if len(existingKeys) > 0 {
		fmt.Println("Redis already contains the following keys for this namespace:")
		for _, k := range existingKeys {
			fmt.Println(" ", k)
		}
		fmt.Println("Please delete them (or run flushdb) and try again.")
		os.Exit(1)
	}

	// Start a worker pool, and shut it down once the finish channel receives a
	// message.
	go func() {
		pool := work.NewWorkerPool(Context{}, 1, namespace, pool)
		pool.Job("testjob", func(c *Context, job *work.Job) error {
			time.Sleep(1 * time.Second)
			fmt.Printf("running job id=%s\n", job.ID)
			return nil
		})

		// Run the pool until "finish" receives a message.
		pool.Start()
		<-finish
		pool.Drain()
		pool.Stop()
		done <- struct{}{}
	}()

	// Enqueue 5 jobs with the same unique key. The first job should always
	// run, and a second job should also run if the first job's already started.
	var enqueuer = work.NewEnqueuer(namespace, pool)
	fmt.Println("Enqueing 5 jobs with the same unique key:")
	for i := 1; i <= 5; i++ {
		j, err := enqueuer.EnqueueUniqueByKey(
			"testjob",
			work.Q{},
			work.Q{"id": 1},
		)
		if err != nil {
			fmt.Printf("%d. error enqueuing job: %v\n", i, err)
		} else if j == nil {
			fmt.Printf("%d. no job returned\n", i)
		} else {
			fmt.Printf("%d. job enqueued id=%s\n", i, j.ID)
		}
	}

	// Commence shutdown.
	fmt.Println("Beginning shut down, waiting for pending tasks to finish.")
	finish <- struct{}{} // drain pending tasks
	<-done               // wait for pending tasks to finish

	// Look for any leftover, stuck queues.
	keys, err := redis.Strings(conn.Do("KEYS", namespace+":*inprogress"))
	must(err)
	if len(keys) > 0 {
		fmt.Println("Stuck in progress queues:")
		for _, k := range keys {
			count, err := redis.Int(conn.Do("LLEN", k))
			must(err)
			fmt.Printf("  * %s (%d items)\n", k, count)

			jobs, err := redis.Strings(conn.Do("LRANGE", k, 0, -1))
			must(err)
			for _, j := range jobs {
				var job map[string]interface{}
				must(json.Unmarshal([]byte(j), &job))
				fmt.Printf("    - job id=%s\n", job["id"])
			}
		}
	}

	fmt.Println("done.")
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
