package consistenthash

type options struct {
	hashFunc        HashFunc
	defaultReplicas uint
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
