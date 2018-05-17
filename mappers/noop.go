package mappers

// Noop is a trivial mapper that returns the input
func Noop(_ interface{}, j interface{}) interface{} {
	return j
}
