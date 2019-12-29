package main

import (
	"encoding/json"
	"fmt"
	"mongoAggregate/aggregate"
	"mongoAggregate/mongoClient"
	output2 "mongoAggregate/output"
	"sort"
	"sync"

	bson2 "go.mongodb.org/mongo-driver/bson"
)

func init() {
	mongoClient.InitMongodb()
}

func main() {
	dataStatResult := make(chan bson2.M)
	var output output2.ResultSlice
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go aggregate.DataAggregate(i, dataStatResult, &wg)
	}

	for value := range dataStatResult {
		output = append(output, output2.OutPut{
			Age:       value["age"].(int32),
			Sex:       value["sex"].(int32),
			Total:     value["total"].(int32),
			AvgSalary: value["avgSalary"].(float64),
		})
		if len(output) == 200 {
			break
		}
	}
	wg.Wait()
	//倒序排列
	sort.Sort(output)
	for _, v := range output {
		result, err := json.Marshal(&v)
		if err != nil {
			fmt.Printf("json.marshal failed, err:", err)
			return
		}
		fmt.Println(string(result))
	}
}
