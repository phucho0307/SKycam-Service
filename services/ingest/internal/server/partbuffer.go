package server

// partBuffer turns an arbitrary sequence of chunks into exactly-sized parts.
// Chunks can straddle part boundaries; flush always receives size bytes, except
// for the final call from finish.
//
// Memory per upload is one part buffer (blob.PartSize), regardless of file size.
// Bytes still buffered when a stream breaks are simply dropped: the client
// resumes from the last flushed part boundary.
type partBuffer struct {
	size  int
	buf   []byte
	flush func([]byte) error
}

func (p *partBuffer) write(data []byte) error {
	for len(data) > 0 {
		if p.buf == nil {
			p.buf = make([]byte, 0, p.size)
		}
		n := min(p.size-len(p.buf), len(data))
		p.buf = append(p.buf, data[:n]...)
		data = data[n:]
		if len(p.buf) == p.size {
			if err := p.flush(p.buf); err != nil {
				return err
			}
			p.buf = p.buf[:0]
		}
	}
	return nil
}

// finish flushes the final short part, if any.
func (p *partBuffer) finish() error {
	if len(p.buf) == 0 {
		return nil
	}
	return p.flush(p.buf)
}
