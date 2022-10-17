package iorpc

import (
	"encoding/binary"
	"io"
)

var (
	requestStartLineSize  = binary.Size(requestStartLine{})
	responseStartLineSize = binary.Size(responseStartLine{})
)

type requestStartLine struct {
	Service      uint32
	HeaderSize   uint32
	ID, BodySize uint64
}

type responseStartLine struct {
	ErrorSize    uint32
	HeaderSize   uint32
	ID, BodySize uint64
}

type wireRequest struct {
	Service uint32
	ID      uint64
	Headers *Buffer
	Size    uint64
	Body    io.ReadCloser
}

type wireResponse struct {
	ID      uint64
	Error   string
	Headers *Buffer
	Size    uint64
	Body    io.ReadCloser
}

type messageEncoder struct {
	w    io.Writer
	stat *ConnStats
}

func (e *messageEncoder) Close() error {
	return nil
}

func (e *messageEncoder) Flush() error {
	return nil
}

func (e *messageEncoder) encode(headers *Buffer, body io.ReadCloser) error {
	if headers != nil && headers.Len() > 0 {
		defer headers.Close()
		n, err := e.w.Write(headers.Bytes())
		if err != nil {
			e.stat.incWriteErrors()
			return err
		}
		e.stat.addHeadWritten(uint64(n))
	}

	if body != nil {
		defer body.Close()
		nc, err := io.Copy(e.w, body)
		if err != nil {
			e.stat.incWriteErrors()
			return err
		}
		e.stat.addBodyWritten(uint64(nc))
	}

	e.stat.incWriteCalls()
	return nil
}

func (e *messageEncoder) EncodeRequest(req wireRequest) error {
	headerSize := 0
	if req.Headers != nil {
		headerSize = req.Headers.Len()
	}

	if err := binary.Write(e.w, binary.BigEndian, requestStartLine{
		ID:         req.ID,
		Service:    req.Service,
		HeaderSize: uint32(headerSize),
		BodySize:   req.Size,
	}); err != nil {
		e.stat.incWriteErrors()
		return err
	}

	e.stat.addHeadWritten(uint64(requestStartLineSize))
	return e.encode(req.Headers, req.Body)
}

func (e *messageEncoder) EncodeResponse(resp wireResponse) error {
	headerSize := 0
	if resp.Headers != nil {
		headerSize = resp.Headers.Len()
	}

	respErr := []byte(resp.Error)

	if err := binary.Write(e.w, binary.BigEndian, responseStartLine{
		ID:         resp.ID,
		ErrorSize:  uint32(len(respErr)),
		HeaderSize: uint32(headerSize),
		BodySize:   resp.Size,
	}); err != nil {
		e.stat.incWriteErrors()
		return err
	}

	e.stat.addHeadWritten(uint64(responseStartLineSize))

	if len(respErr) > 0 {
		n, err := e.w.Write(respErr)
		if err != nil {
			e.stat.incWriteErrors()
			return err
		}
		e.stat.addHeadWritten(uint64(n))
	}

	return e.encode(resp.Headers, resp.Body)
}

func newMessageEncoder(w io.Writer, s *ConnStats) *messageEncoder {
	return &messageEncoder{
		w:    w,
		stat: s,
	}
}

type messageDecoder struct {
	closeBody bool
	r         io.Reader
	stat      *ConnStats
}

func (d *messageDecoder) Close() error {
	return nil
}

func (d *messageDecoder) DecodeRequest(req *wireRequest) error {
	var startLine requestStartLine
	if err := binary.Read(d.r, binary.BigEndian, &startLine); err != nil {
		d.stat.incReadErrors()
		return err
	}
	d.stat.addHeadRead(uint64(requestStartLineSize))

	req.ID = startLine.ID
	req.Service = startLine.Service
	req.Size = startLine.BodySize

	if startLine.HeaderSize > 0 {
		req.Headers = GetBuffer()
		if _, err := io.CopyN(req.Headers, d.r, int64(startLine.HeaderSize)); err != nil {
			d.stat.incReadErrors()
			return err
		}
		d.stat.addHeadRead(uint64(startLine.HeaderSize))
	}

	if req.Size > 0 {
		buf := GetBuffer()
		bytes, err := buf.ReadFrom(io.LimitReader(d.r, int64(req.Size)))
		if err != nil {
			return err
		}
		d.stat.addBodyRead(uint64(bytes))
		if d.closeBody {
			buf.Close()
		} else {
			req.Body = buf
		}
	}
	d.stat.incReadCalls()
	return nil
}

func (d *messageDecoder) DecodeResponse(resp *wireResponse) error {
	var startLine responseStartLine
	if err := binary.Read(d.r, binary.BigEndian, &startLine); err != nil {
		d.stat.incReadErrors()
		return err
	}
	d.stat.addHeadRead(uint64(responseStartLineSize))

	resp.ID = startLine.ID
	resp.Size = startLine.BodySize

	if startLine.ErrorSize > 0 {
		respErr := make([]byte, startLine.ErrorSize)
		if _, err := io.ReadFull(d.r, respErr); err != nil {
			d.stat.incReadErrors()
			return err
		}
		d.stat.addHeadRead(uint64(startLine.ErrorSize))
		resp.Error = string(respErr)
	}

	if startLine.HeaderSize > 0 {
		resp.Headers = GetBuffer()
		if _, err := io.CopyN(resp.Headers, d.r, int64(startLine.HeaderSize)); err != nil {
			d.stat.incReadErrors()
			return err
		}
		d.stat.addHeadRead(uint64(startLine.HeaderSize))
	}

	if resp.Size > 0 {
		buf := GetBuffer()
		bytes, err := buf.ReadFrom(io.LimitReader(d.r, int64(resp.Size)))
		if err != nil {
			return err
		}
		d.stat.addBodyRead(uint64(bytes))
		if d.closeBody {
			buf.Close()
		} else {
			resp.Body = buf
		}
	}
	d.stat.incReadCalls()
	return nil
}

func newMessageDecoder(r io.Reader, s *ConnStats, closeBody bool) *messageDecoder {
	return &messageDecoder{
		r:         r,
		stat:      s,
		closeBody: closeBody,
	}
}
