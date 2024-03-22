package scheduler

type UnionSet struct {
	Father []uint32
	Rank   []uint32
}

func NewUnionSet() *UnionSet {
	father := make([]uint32, 0)
	rank := make([]uint32, 0)

	//for i := uint32(0); i < size; i++ {
	//	father[i] = i
	//	rank[i] = 0
	//}

	return &UnionSet{
		Father: father,
		Rank:   rank,
	}
}

func (u *UnionSet) AddUnionSet(i uint32) {
	u.Father = append(u.Father, i)
	u.Rank = append(u.Rank, 0)
}

func (u *UnionSet) Find(x uint32) uint32 {
	if x == u.Father[x] {
		return x
	}

	u.Father[x] = u.Find(u.Father[x])
	return u.Father[x]
}

func (u *UnionSet) Merge(x, y uint32) {
	rootX := u.Find(x)
	rootY := u.Find(y)

	if rootX != rootY {
		if u.Rank[rootX] < u.Rank[rootY] {
			rootX, rootY = rootY, rootX
		}
		u.Father[rootY] = rootX
		if u.Rank[rootX] == u.Rank[rootY] {
			u.Rank[rootX] += 1
		}
	}
}
