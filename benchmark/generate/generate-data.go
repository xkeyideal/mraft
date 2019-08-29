package generate

import (
	"crypto/rand"
	"io"
	mrand "math/rand"
	"mraft/store"
	"time"
)

var idChars = []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")

const idLen = 20

func GenerateData() *store.RaftAttribute {
	attr := &store.RaftAttribute{
		AttrID:    uint64(mrand.Int31n(1000000) + 1000000),
		AttrName:  randomId(),
		Ages:      []int32{},
		Locations: make(map[string]string),
		Timestamp: time.Now().UnixNano(),
	}

	l := mrand.Intn(100) + 1
	for i := 0; i < l; i++ {
		attr.Ages = append(attr.Ages, mrand.Int31n(200)+1)
	}

	n := mrand.Intn(50) + 1
	for i := 0; i < n; i++ {
		attr.Locations[randomId()] = randomId()
	}

	return attr
}

// randomId returns a new random id string.
func randomId() string {
	b := randomBytesMod(idLen, byte(len(idChars)))
	for i, c := range b {
		b[i] = idChars[c]
	}
	return string(b)
}

func randomBytes(length int) (b []byte) {
	b = make([]byte, length)
	io.ReadFull(rand.Reader, b)
	return
}

func randomBytesMod(length int, mod byte) (b []byte) {
	maxrb := 255 - byte(256%int(mod))
	b = make([]byte, length)
	i := 0
	for {
		r := randomBytes(length + (length / 4))
		for _, c := range r {
			if c > maxrb {
				continue
			}
			b[i] = c % mod
			i++
			if i == length {
				return b
			}
		}
	}
}
