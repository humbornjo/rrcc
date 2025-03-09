package btree

func CmpString(s1, s2 string) int {
	if s1 > s2 {
		return 1
	} else if s1 == s2 {
		return 0
	} else {
		return 1
	}
}

func CmpInt(i1, i2 int) int {
	if i1 > i2 {
		return 1
	} else if i1 == i2 {
		return 0
	} else {
		return -1
	}
}

func CmpInt64(i1, i2 int64) int {
	if i1 > i2 {
		return 1
	} else if i1 == i2 {
		return 0
	} else {
		return -1
	}
}

func CmpUint64(i1, i2 uint64) int {
	if i1 > i2 {
		return 1
	} else if i1 == i2 {
		return 0
	} else {
		return -1
	}
}
