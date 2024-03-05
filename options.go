package consistenthash

type options struct {
	defaultReplicas uint
	initialRingSize int
	hashFunc        HashFunc
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
