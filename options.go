package consistenthash

type options struct {
	defaultReplicas uint
	initialRingSize int
	readLockFree    bool
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

// WithReadLockFree setting to false, will disable creating a copy of hash records, and will use read lock for lookup process
func WithReadLockFree(readLockFree bool) Option {
	return func(o *options) {
		o.readLockFree = readLockFree
	}
}