package IDB

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func copyMap(m map[int]bool) map[int]bool {
	r := make(map[int]bool)
	for k, b := range m {
		r[k] = b
	}
	return r
}
