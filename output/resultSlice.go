package output

// 按照 Person.Age 从大到小排序
type OutPut struct {
	Age int32 `json:"age"`
	Sex int32 `json:"sex"`
	Total int32 `json:"total"`
	AvgSalary float64 `json:"avg_salary"`
}

type ResultSlice [] OutPut

func (a ResultSlice) Len() int { // 重写 Len() 方法
	return len(a)
}
func (a ResultSlice) Swap(i, j int) { // 重写 Swap() 方法
	a[i], a[j] = a[j], a[i]
}
func (a ResultSlice) Less(i, j int) bool { // 重写 Less() 方法， 从大到小排序
	return a[j].Age < a[i].Age
}
