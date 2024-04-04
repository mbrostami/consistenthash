package consistenthash

// Option represents a function that configures options for consistent hashing
type Option func(*options)

type options struct {
	hashFunc          HashFunc
	defaultReplicas   uint
	blockPartitioning int
}

// WithDefaultReplicas configures the default number of replicas to add for each key
func WithDefaultReplicas(replicas uint) Option {
	return func(o *options) {
		o.defaultReplicas = replicas
	}
}

// WithHashFunc configures the hash function for 32bit CH
func WithHashFunc(hashFunc HashFunc) Option {
	return func(o *options) {
		o.hashFunc = hashFunc
	}
}

// WithBlockPartitioning configures block partitioning to divide total number of keys into blocks
func WithBlockPartitioning(divisionBy int) Option {
	return func(o *options) {
		o.blockPartitioning = divisionBy
	}
}
