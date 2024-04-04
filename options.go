package consistenthash

// options struct holds the configurable options for consistent hash
type options struct {
	hashFunc          HashFunc
	defaultReplicas   uint
	blockPartitioning int
}

// Option is a function type used to apply configurable options to consistent hash
type Option func(*options)

// WithDefaultReplicas sets the default number of replicas to add for each key
func WithDefaultReplicas(replicas uint) Option {
	return func(o *options) {
		o.defaultReplicas = replicas
	}
}

// WithHashFunc sets the hash function for 32bit CH
func WithHashFunc(hashFunc HashFunc) Option {
	return func(o *options) {
		o.hashFunc = hashFunc
	}
}

// WithBlockPartitioning applies block partitioning by dividing total number of keys
func WithBlockPartitioning(divisionBy int) Option {
	return func(o *options) {
		o.blockPartitioning = divisionBy
	}
}
