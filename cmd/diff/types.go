package diff

type keySet interface {
	Contains(string) bool
	Add(string)
	Len() int
}

func newKeyMap() keySet { return make(keymap) }

type keymap map[string]struct{}

func (k keymap) Contains(key string) bool { _, ok := k[key]; return ok }
func (k keymap) Add(key string)           { k[key] = struct{}{} }
func (k keymap) Len() int                 { return len(k) }
