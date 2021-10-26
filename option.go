package bitcask

const (
	defaultMaxFileSize = 1 << 31 // 2G
)

type Option struct {
	MaxFileSize uint64
	MergeSecs   int
}

func NewOption(MaxFileSize uint64) Option {
	if MaxFileSize <= 0 {
		MaxFileSize = defaultMaxFileSize
	}
	return Option{MaxFileSize: MaxFileSize}
}
