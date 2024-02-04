package testutils

type Option interface {
	ApplyTo(opts *Options)
}

type Options struct {
	numWorker  int
	debugLevel int
}

type workerOpt int

func NumWorkers(n int) Option {
	return workerOpt(n)
}

func (o workerOpt) ApplyTo(opts *Options) {
	opts.numWorker = int(o)
}

type debugLevel int

func DebugLevel(n int) Option {
	return debugLevel(n)
}

func (o debugLevel) ApplyTo(opts *Options) {
	opts.debugLevel = int(o)
}
