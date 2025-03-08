package rrcc

type event_t int

const (
	ADD event_t = iota // key is newly set
	CHG                // key is changed to a new value
	DEL                // key is deleted
)

type event struct {
	version uint64 // internal event to merge a set of events

	OldValue string
	NewValue string
	Type     event_t
}

func mkEventAdd(value string, ver uint64) event {
	return event{
		version:  ver,
		Type:     ADD,
		NewValue: value,
	}
}

func mkEventChg(oldValue, newValue string, ver uint64) event {
	return event{
		version:  ver,
		Type:     CHG,
		OldValue: oldValue,
		NewValue: newValue,
	}
}

func mkEventDel(value string, ver uint64) event {
	return event{
		version:  ver,
		Type:     DEL,
		OldValue: value,
	}
}

func (p event) mergeEvents(events []event) (event, error) {
	if len(events) == 0 {
		return p, errUnchanged
	}

	last := events[len(events)-1]
	switch p.Type {
	case DEL:
		// if p's type is DEL, then new event is
		//   - DEL if last events are DEL, should return errUnchange
		//   - ADD otherwise, mkEventAdd(<LAST_EVENT_NEW>)
		if last.Type == DEL {
			if last.version != p.version {
				p.version = last.version
			}
			return p, errUnchanged
		}
		return mkEventAdd(last.NewValue, last.version), nil
	case ADD, CHG:
		// if p's type is ADD/CHG, then new event is
		//   - DEL if last events are DEL, mkEventDel(<P_EVENT_NEW>)
		//   - CHG otherwise.
		if last.Type == DEL {
			return mkEventDel(p.NewValue, last.version), nil
		}

		if last.NewValue == p.NewValue {
			if last.version != p.version {
				p.version = last.version
			}
			return p, errUnchanged
		}
		return mkEventChg(p.NewValue, last.NewValue, last.version), nil
	default:
		panic("unreachable")
	}
}
