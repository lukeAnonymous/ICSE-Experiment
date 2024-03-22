package hotcache

type Hotcache struct {
	Blk_num uint64

	Recorder          *RWRecorder
	Node_disk_get_num uint64
}

func NewHotcache(size int, bitsPerKey int) *Hotcache {

	hc := &Hotcache{}

	return hc

}
