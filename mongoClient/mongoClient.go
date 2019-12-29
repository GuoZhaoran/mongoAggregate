package mongoClient

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoClient struct {
	Client *mongo.Client
	Collection *mongo.Collection
}

var (
	GMongo *MongoClient
)

func InitMongodb()  {
	var(
		ctx context.Context
		opts *options.ClientOptions
		client *mongo.Client
		err error
		collection *mongo.Collection
	)
	// 连接数据库
	ctx, _ = context.WithTimeout(context.Background(), 10*time.Second)  // ctx
	opts = options.Client().ApplyURI("mongodb://127.0.0.1:27017")  // opts
	if client, err = mongo.Connect(ctx,opts); err != nil{
		fmt.Println(err)
		return
	}

	//链接数据库和表
	collection = client.Database("screen_data_stat").Collection("test")

	//赋值单例
	GMongo = &MongoClient{
		Client:client,
		Collection:collection,
	}
}