package event

import "errors"

type event_t int

var ErrUnchanged = errors.New("unchanged")

const (
	DEL event_t = iota // Key deleted
	ADD                // Key newly set
	CHG                // Key changed to a new value
)

type Event struct {
	Version  int64 // internal event to merge a set of events
	OldValue string
	NewValue string
	Type     event_t
}

func MkEventAdd(value string, ver int64) Event {
	return Event{
		Version:  ver,
		NewValue: value,
		Type:     ADD,
	}
}

func MkEventChg(oldValue, newValue string, ver int64) Event {
	return Event{
		Version:  ver,
		OldValue: oldValue,
		NewValue: newValue,
		Type:     CHG,
	}
}

func MkEventDel(value string, ver int64) Event {
	return Event{
		Version:  ver,
		OldValue: value,
		Type:     DEL,
	}
}

func (p Event) MergeEvents(events []Event) (Event, error) {
	if len(events) == 0 {
		return p, ErrUnchanged
	}

	last := events[len(events)-1]
	if last.Version < p.Version {
		return p, ErrUnchanged
	}

	switch p.Type {
	case DEL:
		// if p's type is DEL, then new event is
		//   - DEL if last events are DEL, should return errUnchange
		//   - ADD otherwise, mkEventAdd(<LAST_EVENT_NEW>)
		if last.Type == DEL {
			if last.Version > p.Version {
				p.Version = last.Version
			}
			return p, ErrUnchanged
		}
		return MkEventAdd(last.NewValue, last.Version), nil
	case ADD, CHG:
		// if p's type is ADD/CHG, then new event is
		//   - DEL if last events are DEL, mkEventDel(<P_EVENT_NEW>)
		//   - CHG otherwise.
		if last.Type == DEL {
			return MkEventDel(p.NewValue, last.Version), nil
		}

		if last.NewValue == p.NewValue {
			if last.Version > p.Version {
				p.Version = last.Version
			}
			return p, ErrUnchanged
		}
		return MkEventChg(p.NewValue, last.NewValue, last.Version), nil
	default:
		panic("unreachable")
	}
}
