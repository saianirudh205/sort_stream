package sort

// verifySorted checks whether packets are sorted by the given key.
// Used by validation and optional runtime verification.
func verifySorted(values []Packet, key string) bool {
	if len(values) < 2 {
		return true
	}

	switch key {
	case "id":
		for i := 1; i < len(values); i++ {
			if values[i-1].ID > values[i].ID {
				return false
			}
		}
	case "name":
		for i := 1; i < len(values); i++ {
			if values[i-1].Name > values[i].Name {
				return false
			}
		}
	case "continent":
		for i := 1; i < len(values); i++ {
			if values[i-1].Continent > values[i].Continent {
				return false
			}
		}
	default:
		return false
	}

	return true
}
