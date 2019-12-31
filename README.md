### 1.前言
大数据的聚合分析在企业中非常有用，有过大数据开发经验的人都知道ES、Mongo都提供了专门的聚合方案来解决这个问题。但是大量数据的实时聚合一直是业务实现上的痛点,ES、Mongo天然对分布式友好，往往将海量数据存储到不同的分片上; Go语言天生为并行而生，数据聚合往往可以将数据分块计算，本节结合Go语言的并行计算特性实现1秒聚合千万mongo文档数据。笔者对大数据没有深入的研究，希望有经验的读者能够提出批评和更多建议。本文托管github的源码地址是: [mongo千万文档高效聚合](https://github.com/GuoZhaoran/mongoAggregate)

### 2.mongo数据库常用聚合方法

mongo没有像mysql⼀样的范式约束,存储的可以是复杂类型，⽐如数组、对象等mysql不善于处理的⽂档型结构，与此同时聚合的操作也⽐mysql复杂很多。

mongo提供了三种⽅式完成⽂档数据聚合操作,本节来总结⼀下三种⽅式的区别:

- 聚合框架(aggregate pipeline)
- 聚合计算模型(MapReduce)
- 单独的聚合命令(group、distinct、count)

#### 2.1 单独的聚合命令

单独的聚合命令⽐aggregate性能低，⽐Mapreduce灵活度低；使⽤起来简单。

- group: 可⽤于⼩数据量的⽂档聚合运算，⽤于提供⽐count、distinct更丰富的统计需求，可以使⽤js函数控制统
计逻辑。

> 在2.2版本之前，group操作最多只能返回10000条分组记录，但是从2.2版本之后到2.4版本，mongodb做了优化，能够⽀持返回20000条分组记录返回，如果分组记录的条数⼤于20000条，那么可能你就需要其他⽅式进⾏统计了，⽐如聚合管道或者MapReduce

- count: db.collection.count() 等同于 db.collection.find().count(), 不能适⽤于分布式环境，分布式环境推荐使⽤
aggregate

- distinct: 可以使⽤到索引,语法⾮常简单：db.collection.distinct(field,query),field是去重字段(单个或嵌套字段
名);query是查询条件

#### 2.2 聚合框架 aggregate pipeline

aggregate 聚合框架是基于数据处理管道(pipeline)模型建⽴，⽂档通过多级管道处理后返回聚合结果；aggregate管道聚合⽅案使⽤的是mongodb内置的汇总操作，相对来说更为⾼效，在做mongodb数据聚合操作的时候优先推荐aggregate;

aggregate能够通过索引来提升性能，还有⼀些具体的技巧来管道性能(aggregate 管道操作是在内存中完成的，有内存⼤⼩限制，处理数据集⼤⼩有限);

aggregate管道操作像unix/Linux系统内的管道操作,将当前⽂档进⼊第⼀个管道节点处理完成后,将处理完成的数据丢给下⼀个管道节点,⼀直到最后处理完成后,输出内容;

> **aggregate的限制**
> 1. 当aggregate返回的结果集中的单个⽂档超过16MB命令会报错(使⽤aggregate不指定游标选项或存储集合中的结果，aggregate命令会返回⼀个包涵于结果集的字段中的bson⽂件。如果结果集的总⼤⼩超过bson⽂件⼤⼩限制（16MB）该命令将产⽣错误；)
> 2. 管道处理阶段有内存限制最⼤不能超过100MB,超过这个限制会报错误；为了能够处理更⼤的数据集可以开启allowDiskUse选项，可以将管道操作写⼊临时⽂件；aggregate的使⽤场景适⽤于对聚合响应性能需要⼀定要求的场景（索引及组合优化)

#### 2.3 聚合计算模型 MapReduce

MapReduce的强⼤之处在于能够在多台Server上并⾏执⾏复杂的聚合逻辑。MapReduce是⼀种计算模型，简单的说就是将⼤批量的⼯作（数据）分解（MAP）执⾏，然后再将结果合并成最终结果（REDUCE）。MapReduce使⽤惯⽤的javascript操作来做map和reduce操作，因此MapReduce的灵活性和复杂性都会⽐aggregate pipeline更⾼⼀些，并且相对aggregate pipeline⽽⾔更消耗性能；MapReduce⼀般情况下会使⽤磁盘存储预处理数
据,⽽pipeline始终在内存处理数据。

> **MapReduce的使⽤场景**
使⽤于处理⼤数据结果集，使⽤javascript灵活度⾼的特点，可以处理复杂聚合需求

### 3.aggregate pipeline 实现原理和常用语法

MongoDB中聚合框架(aggregate pipeline)的⽅法使⽤aggregate(),语法如下:

> db.COLLECTION_NAME.aggregate(AGGREGATE_OPERATION)

下边是aggregate()⽅法与mysql聚合类⽐

mongo聚合操作 | SQL操作(函数) | 说明 | 
---- | --- | ---
$match | where | 对数据进行条件搜索
$group |  group by | 对数据进行分组聚合
$having | having | 对聚合后的数据进行过滤筛选
$project | select | 选择数据字段
$sort | order by | 对数据进行排序
$limit | limit | 限制数据返回数量
$sum | sum()、count() | 聚合统计数据字段


aggregate中\$match、\$group等操作被称为pipeline中的stage(阶段)，它们提供了丰富的⽅法来筛选聚合数据,\$match提供了\$gt(>)、\$lt(<)、\$in(in)、\$nin(not in)、\$gte(>=)、\$lte(<=)等等筛选符。

**\$group** 按指定的表达式对⽂档进⾏分组，并将每个不同分组的⽂档输出到下⼀个阶段。输出⽂档包含⼀个_id字段，该字段按键包含不同的组。输出⽂档还可以包含计算字段，该字段保存由\$group的_id字段分组的⼀些accumulator表达式的值。 \$group不会输出具体的⽂档⽽只是统计信息。语法:

> { $group: { _id: \<expression>, \<field1>: { \<accumulator1> : \<expression1> }, ... } }

- _id字段是必填的;但是，可以指定_id值为null来为整个输⼊⽂档计算累计值。
- 剩余的计算字段是可选的，并使⽤<accumulator>运算符进⾏计算。

accumulator常⽤操作符:

名称 | 描述 | sql类比 | 
---- | --- | ---
$avg  | 计算平均值 avg | avg
$first |  返回每组第⼀个⽂档，如果有排序，按照排序，如果没有按照默认的存储的顺序的第⼀个⽂档。| limit 0,1
$last | 返回每组最后⼀个⽂档，如果有排序，按照排序，如果没有按照默认的存储的顺序的最后个⽂档。 | -
$max | 根据分组，获取集合中所有⽂档对应值得最⼤值。 | max
$min | 根据分组，获取集合中所有⽂档对应值得最⼩值。 | min
$sum | 计算总和  | sum
$push | 将指定的表达式的值添加到⼀个数组中。 | - 

db.collection.aggregate()是基于数据处理的聚合管道，每个⽂档通过⼀个由多个阶段（stage）组成的管道，可以对每个阶段的管道进⾏分组、过滤等功能，然后经过⼀系列的处理，输出相应的结果。通过这张图，可以了解Aggregate处理的过程:

![](https://user-gold-cdn.xitu.io/2019/12/31/16f5c76b279891a6?w=1596&h=1138&f=png&s=267105)

聚合管道可以检测到是否仅使⽤⽂档中的⼀部分字段就可以完成聚合。如果是的话，管道就可以仅使⽤这些必要的字段，从⽽减少进⼊管道的数据量。

下⾯列举⼏个常⻅的优化技巧:

- 1.\$match + \$group 顺序优化
在管道中\$group千⾯使⽤\$match对⽂档数据做筛选，能⼤幅度减少单个pipeline返回⽂档的数量，从⽽提升效率
- 2.\$group + \$project 优化
\$group 管道对⽂档数据聚合之后，默认会返回⼀个_id的bson⽂档,我们可以将_id中使⽤到的数据导出,在\$project中
只设置限制指定字段，可以减少输出⽂档⼤⼩
- 3.\$skip + \$limit 优化
如果你的管道中， \$skip 后⾯跟着 \$limit ，优化器会把 \$limit 移到 \$skip 前⾯，这个时候， \$limit 的值会加上 \$skip
的个数。
- 4.如果 \$sort 在 \$limit 前⾯，优化器可以把 \$limit 合并在 \$sort 内部。此时如果指定了限定返回 n 个结果，那么
排序操作仅需要维护最前⾯的 n 个结果，MongoDB只需要在内存中存储 n 个元素

关于更多的聚合优化技巧，可查看: [mongo聚合优化](http://www.mongoing.com/docs/reference/operator/aggregation/sort.html#sort-and-memory). 

### 4. 代码实现

#### 4.1 数据整理
本节代码演示需要用到大量的数据，大家可以使用mysql的存储过程生成海量数据,然后导入到mongo数据库中,生成方法可以参考：[mysql快速生成百万数据](https://segmentfault.com/a/1190000012918964)

```
{
    "_id" : ObjectId("5e06de309d1f74e9badda0db"),
    "username" : "dvHPRGD1",
    "age" : 87,
    "sex" : 1,
    "salary" : 3251
}
{
    "_id" : ObjectId("5e06de309d1f74e9badda0dc"),
    "username" : "rNx6NsK",
    "age" : 7,
    "sex" : 1,
    "salary" : 7891
}
......
```

文档非常简单，随机生成了姓名（username）、年龄(age)、性别(sex)、薪资四个内容(salary)，其中年龄是0-99的随机数，性别只有0和1，薪资也是一定范围的随机数。

#### 4.2 实现目标和解决思路

有了数据源，我们的目标也很简单，就是快速得到不同年龄、不同性别的人的总数和薪资平均值。其实就是要求我们对这千万数据中年龄和性别做聚合，然后再对薪资做统计计算。

目标明确了之后，我们给文档中的年龄和性别添加索引，加快我们的统计。考虑到我们要快速得出结果，所以我们使用mongo的聚合管道aggregate,之前又说过聚合管道有着内存、返回文档大小的限制，一千万的数据绝对会超过mongo对内存的限制使用，为了解决问题，**开发人员往往会通过allDiskUse参数打开磁盘来完成数据的聚合工作，但是磁盘和内存的运算效率相差百倍，势必会影响到聚合效率，无法保证实时性。**

我们换个思路来解决这个问题,虽然我们的文档数据很多，但是年龄是有限的，只有0-99岁100个数，性别也只有男女两种情况，我们可以使用go开启100个goroutine分别聚合age在0-99的文档数据，聚合完成后再将数据给整合到一起就可以完成我们的聚合工作了。go非常适合做这种工作，因为开启goroutine的代价是很少的，再就是如果数据是分布式存储到不同的机器上的，又可以实现数据的分布式聚合。**聚合任务又刚刚好可以分成一个个的小任务，为go语言的并行计算提供了前提条件,一切看起来都是刚刚好。**

#### 4.3 代码解读

笔者条件有限，就使用自己计算机本地数据库演示了，首先创建一个mongo连接的单例(如果是分布式搭建的环境，可以使用连接池)

```
// mongoAggregate/mongoClient/mongoClient.go
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
...... //入口文件main.go中初始化(init函数) Mongo连接
func init() {
	mongoClient.InitMongodb()
}
```

聚合函数实现在aggregate包中:

```
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
```

genPipeline方法用于生成mongo聚合管道的各个阶段，因为go语言可以返回多值，所以在DataAggregate中使用多值接收，将聚合后的结果通过通道resultChan传出去,完成聚合，sync.WaitGroup是为了控制主函数先于其他goroutine先退出而设置的，用于控制并发数量。

因为我们使用多个goroutine并发运算，我们得到结果实际上取决于最慢的那个goroutine完成任务所消耗的时间，我们对结果进行如下处理：排序、格式化为json，那么我们就需要对输出内容做如下定义：

```
//output/resultSlice.go
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
```

上边实现了排序函数接口，我们就可以实现输出结果根据年龄做排序了。

接下来主函数所做的工作就比较清晰了:

```
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
```

首先定义一个管道，用于主goroutine和其他并发goroutine通信，用于接受其他goroutine计算好的结果，例子中开启100个goroutine进行分组聚合，聚合后的结果通过dataStatResult通道接收，转化为Output结构体，存放到切片中，所有的工作完成之后，对结果按照年龄排序，格式化为json输出。这就是并发聚合海量数据的逻辑了。下边是笔者聚合0-20岁的结果(数据大概有200万，200ms就完成了聚合工作):

```
{"age":19,"sex":0,"total":49773,"avg_salary":5346.04197054628}
{"age":19,"sex":1,"total":49985,"avg_salary":4677.7744523357005}
{"age":18,"sex":0,"total":48912,"avg_salary":5335.430671409879}
{"age":18,"sex":1,"total":50136,"avg_salary":4540.624461464816}
{"age":17,"sex":0,"total":49609,"avg_salary":5372.679755689492}
......
```

### 5.小结

本文主要讲述了go语言在大数据聚合统计场景下的应用,其实不管是不是对实时性有要求的场景都有着分块聚合思想的存在，mongo的MapReduce聚合、ES的bucketing(桶聚合)，都是将大数据聚合分批成小任务，一个个完成，最终完成目标，它们的效率并不高。Go语言的并行计算能很好的应用在这个场景下，为海量数据(亿级)数据的实时聚合提供了解决方案。