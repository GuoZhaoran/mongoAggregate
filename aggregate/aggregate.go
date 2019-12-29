package aggregate

import (
	"context"
	"log"
	"mongoAggregate/mongoClient"
	"sync"
	"time"

	bson2 "go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func genPipeline(age int) (bson2.D, bson2.D, bson2.D) {
	matchStage := bson2.D{
		{"$match", bson2.D{
			{"age",
				bson2.D{
					{"$eq", age},
				}},
		}},
	}
	groupStage := bson2.D{
		{"$group", bson2.D{
			{"_id", bson2.D{
				{"age", "$age"},
				{"sex", "$sex"},
			}},
			{"age", bson2.D{
				{"$first", "$age"},
			}},
			{"sex", bson2.D{
				{"$first", "$sex"},
			}},
			{"total", bson2.D{
				{"$sum", 1},
			}},
			{"avgSalary", bson2.D{
				{"$avg", "$salary"},
			}},
		}},
	}
	projectStage := bson2.D{
		{"$project", bson2.D{
			{"_id", 0},
			{"age", 1},
			{"sex", 1},
			{"total", 1},
			{"avgSalary", 1},
		}},
	}

	return matchStage, groupStage, projectStage
}

func DataAggregate(age int, resultChan chan bson2.M, wg *sync.WaitGroup) {
	matchStage, groupStage, projectStage := genPipeline(age)
	opts := options.Aggregate().SetMaxTime(15 * time.Second)
	cursor, err := mongoClient.GMongo.Collection.Aggregate(context.TODO(), mongo.Pipeline{matchStage, groupStage, projectStage}, opts)
	if err != nil {
		log.Fatal(err)
	}

	//打印文档内容
	var results []bson2.M
	if err = cursor.All(context.TODO(), &results); err != nil {
		log.Fatal(err)
	}
	for _, result := range results {
		resultChan <- result
	}
	wg.Done()
}
