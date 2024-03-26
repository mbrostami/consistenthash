package consistenthash

type options struct {
	hashFunc          HashFunc
	initialRingSize   int
	defaultReplicas   uint
	blockPartitioning int
	metrics           bool
	readLockFree      bool
}

type Option func(*options)

// WithDefaultReplicas default number of replicas to add for each key
func WithDefaultReplicas(replicas uint) Option {
	return func(o *options) {
		o.defaultReplicas = replicas
	}
}

// WithHashFunc hash function for 32bit CH
func WithHashFunc(hashFunc HashFunc) Option {
	return func(o *options) {
		o.hashFunc = hashFunc
	}
}

// WithBlockPartitioning uses block partitioning, divides total number of keys to the given number to get number of blocks
func WithBlockPartitioning(divisionBy int) Option {
	return func(o *options) {
		o.blockPartitioning = divisionBy
	}
}

// WithMetrics enables collecting metrics for block partitioning
// using metrics is not thread safe, and needs to be used only for test and debugging
func WithMetrics() Option {
	return func(o *options) {
		o.metrics = true
	}
}
