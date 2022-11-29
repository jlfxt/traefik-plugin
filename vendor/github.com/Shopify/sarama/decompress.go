package sarama

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"sync"

	snappy "github.com/eapache/go-xerial-snappy"
)

var (
	gzipReaderPool sync.Pool
)

func decompress(cc CompressionCodec, data []byte) ([]byte, error) {
	switch cc {
	case CompressionNone:
		return data, nil
	case CompressionGZIP:
		var err error
		reader, ok := gzipReaderPool.Get().(*gzip.Reader)
		if !ok {
			reader, err = gzip.NewReader(bytes.NewReader(data))
		} else {
			err = reader.Reset(bytes.NewReader(data))
		}

		if err != nil {
			return nil, err
		}

		defer gzipReaderPool.Put(reader)

		return io.ReadAll(reader)
	case CompressionSnappy:
		return snappy.Decode(data)
	case CompressionLZ4:
		return []byte{}, nil
	case CompressionZSTD:
		return zstdDecompress(ZstdDecoderParams{}, nil, data)
	default:
		return nil, PacketDecodingError{fmt.Sprintf("invalid compression specified (%d)", cc)}
	}
}
