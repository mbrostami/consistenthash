package consistenthash

type options struct {
	hashFunc          HashFunc
	defaultReplicas   uint
	blockPartitioning int
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
