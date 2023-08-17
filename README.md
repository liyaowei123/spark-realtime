# spark-realtime
### 跟着尚硅谷spark-streaming项目做的  
### 项目架构：  
开发语言：Scala、Java  
开发框架：Spring-Boot、Echart  
计算框架：Spark-Streaming  
数据库：Redis、Elasticsearch  
消息队列：Kafka  
数据采集：Maxwell（离线）、Spark-Streaming（实时）  
  
### 项目流程：  
1、产生数据到MySQL；  
2、使用Maxwell把数据从MySQL采集到Kafka；  
3、ODS层Spark-Streaming从Kafka消费数据，对消费的数据进行分流处理，维度数据写入Redis，事实数据重新写入Kafka的不同主题；  
4、DWD层Spark-Streaming再从相应的Kafka主题中消费数据，进行数据处理，写入到Elasticsearch；  
5、通过Spring-Boot开发相关接口，从写入到Elasticsearch中读取数据并展示。  
  
### 项目亮点：  
解决从Kafka中消费数据时的漏消费、重复消费以及读取数据时的顺序问题。  
