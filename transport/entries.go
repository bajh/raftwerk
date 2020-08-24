package transport

func NewSetOp(key string, val string, term int64, index int64) *Entry {
	return &Entry{
		Term:  term,
		Index: index,
		Entry: &Entry_SetOp{
			SetOp: &SetOp{
				Key: key,
				Val: val,
			},
		},
	}
}
