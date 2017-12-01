package elastic

const (
	ErrorNotFound = iota
	ErrorIndexAreadyExists
)

type Error struct {
	typ     uint8
	message string
}

func (e Error) Error() string {
	return e.message
}

func IsNotFound(err error) bool {
	e, ok := err.(Error)
	return ok && e.typ == ErrorNotFound
}

func IsIndexAreadyExists(err error) bool {
	e, ok := err.(Error)
	return ok && e.typ == ErrorIndexAreadyExists
}
