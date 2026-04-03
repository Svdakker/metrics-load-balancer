package dispatcher

import (
	"context"

	"github.com/prometheus/prometheus/prompb"

	"github.com/Svdakker/metrics-load-balancer/internal/client"
)

type Job struct {
	Ctx     context.Context
	Target  string
	Payload *prompb.WriteRequest
	Result  chan error
}

type Dispatcher struct {
	client      *client.Client
	workerCount int
	jobQueue    chan Job
}

func New(c *client.Client, workerCount int, bufferSize int) *Dispatcher {
	return &Dispatcher{
		client:      c,
		workerCount: workerCount,
		jobQueue:    make(chan Job, bufferSize),
	}
}

func (d *Dispatcher) Start() {
	for i := 0; i < d.workerCount; i++ {
		go d.worker()
	}
}

func (d *Dispatcher) worker() {
	for job := range d.jobQueue {
		payloadBytes, err := d.client.Pack(job.Payload)
		if err != nil {
			job.Result <- err
			continue
		}

		err = d.client.Push(job.Ctx, job.Target, payloadBytes)

		job.Result <- err
	}
}

func (d *Dispatcher) Submit(j Job) {
	d.jobQueue <- j
}
