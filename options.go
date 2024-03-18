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

func WithDefaultReplicas(replicas uint) Option {
	return func(o *options) {
		o.defaultReplicas = replicas
	}
}

func WithHashFunc(hashFunc HashFunc) Option {
	return func(o *options) {
		o.hashFunc = hashFunc
	}
}

// WithReadLockFree setting to false, will disable creating a copy of hash records, and will use read lock for lookup process
func WithReadLockFree(readLockFree bool) Option {
	return func(o *options) {
		o.readLockFree = readLockFree
	}
}

// WithBlockPartitioning uses block partitioning, divides total number of keys to divisionBy to get the number of blocks
func WithBlockPartitioning(divisionBy int) Option {
	return func(o *options) {
		o.blockPartitioning = divisionBy
	}
}

// WithMetrics enables collecting metrics for block partitioning
func WithMetrics() Option {
	return func(o *options) {
		o.metrics = true
	}
}
