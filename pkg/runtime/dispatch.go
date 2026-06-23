package runtime

import (
	"sync"

	"github.com/scitrera/agent-harness-go/pkg/channel"
)

// keyedDispatcher runs tasks concurrently across keys (thread ids) while
// serializing tasks within a key, bounded by a global concurrency limit. Per
// key, a single goroutine drains that key's queue in order; across keys, up to
// `concurrency` tasks run at once (a semaphore). Idle keys are removed so no
// goroutines or queues leak.
type keyedDispatcher struct {
	run   func(channel.Inbound)
	keyOf func(channel.Inbound) string
	sem   chan struct{}

	mu      sync.Mutex
	pending map[string][]channel.Inbound
	active  map[string]bool
	wg      sync.WaitGroup
}

func newKeyedDispatcher(concurrency int, run func(channel.Inbound), keyOf func(channel.Inbound) string) *keyedDispatcher {
	if concurrency < 1 {
		concurrency = 1
	}
	return &keyedDispatcher{
		run:     run,
		keyOf:   keyOf,
		sem:     make(chan struct{}, concurrency),
		pending: map[string][]channel.Inbound{},
		active:  map[string]bool{},
	}
}

// submit routes a task: if its key is already being processed, it queues behind
// the in-flight task; otherwise it starts a worker goroutine for that key.
func (d *keyedDispatcher) submit(env channel.Inbound) {
	key := d.keyOf(env)
	d.mu.Lock()
	if d.active[key] {
		d.pending[key] = append(d.pending[key], env)
		d.mu.Unlock()
		return
	}
	d.active[key] = true
	d.mu.Unlock()
	d.launch(key, env)
}

func (d *keyedDispatcher) launch(key string, env channel.Inbound) {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		cur := env
		for {
			d.sem <- struct{}{}
			d.run(cur)
			<-d.sem

			d.mu.Lock()
			queue := d.pending[key]
			if len(queue) == 0 {
				delete(d.active, key)
				delete(d.pending, key)
				d.mu.Unlock()
				return
			}
			cur = queue[0]
			d.pending[key] = queue[1:]
			d.mu.Unlock()
		}
	}()
}

// wait blocks until all submitted work has completed.
func (d *keyedDispatcher) wait() { d.wg.Wait() }
