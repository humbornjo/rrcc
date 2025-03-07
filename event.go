package rrcc

type event_t int

const (
	ping event_t = iota
	ADD
	MODIFY
)

type event struct {
	OldValue string
	NewValue string
	Type     event_t
}

func mkEventAdd(value string) event {
	return event{
		NewValue: value,
		Type:     ADD,
	}
}

func mkEventModify(oldValue, newValue string) event {
	return event{
		OldValue: oldValue,
		NewValue: newValue,
		Type:     MODIFY,
	}
}
