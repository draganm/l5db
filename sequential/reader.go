package sequential

import (
	"io"

	"github.com/pkg/errors"
)

type reader struct {
	d   data
	pos int
}

func (r *reader) Read(p []byte) (n int, err error) {
	payload := r.d.payload()[r.pos:]

	if len(payload) == 0 {
		if !r.d.hasNextBlock() {
			return 0, io.EOF
		}
		bl, err := r.d.nextBlock()
		if err != nil {
			return 0, errors.Wrap(err, "while getting next block")
		}
		r.d = bl
		r.pos = 0

		payload = r.d.payload()[r.pos:]
	}

	toDo := len(payload)

	if len(p) < len(payload) {
		toDo = len(p)
	}

	copy(p, payload)
	r.pos += toDo
	return toDo, nil

}
