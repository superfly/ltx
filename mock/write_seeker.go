package mock

type WriteSeeker struct {
	WriteFunc func(p []byte) (n int, err error)
	SeekFunc  func(offset int64, whence int) (int64, error)
}

func (w *WriteSeeker) Write(p []byte) (n int, err error) {
	return w.WriteFunc(p)
}

func (w *WriteSeeker) Seek(offset int64, whence int) (int64, error) {
	return w.SeekFunc(offset, whence)
}
