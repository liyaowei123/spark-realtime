package edu.lyw.utils

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.persistent.UpdatePersistentTaskStatusAction.RequestBuilder
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder

import java.util.Map
import scala.collection.mutable.ListBuffer

object MyEsUtil {

  private val esClient: RestHighLevelClient = build()
  /**
   * 创建客户端
   * */
  def build(): RestHighLevelClient = {
    val host: String = MyPropertiesUtil(MyConfig.ES_HOST)
    val port: String = MyPropertiesUtil(MyConfig.ES_PORT)
    val client: RestHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(host, port.toInt)))
    client
  }
  /**
   * 关闭客户端
   * */
  def close(): Unit = if (esClient != null) esClient.close()
  /**
   * 分批写入
   * 幂等写入
   * */
  def bulkSave(indexName: String, docs: List[(String, AnyRef)]): Unit = {
    val request = new BulkRequest(indexName)
    for ((docId, docObj) <- docs) {
      val ir: IndexRequest = new IndexRequest()
      ir.id(docId)
      ir.source(JSON.toJSONString(docObj, new SerializeConfig(true)), XContentType.JSON)
      request.add(ir)
    }
    esClient.bulk(request, RequestOptions.DEFAULT)
  }
  /**
   * 查询指定字段
   * */
  def search(indexName: String, fieldName: String): List[String] = {
    // 如果索引不存在就返回
    val indexRequest: GetIndexRequest = new GetIndexRequest(indexName)
    val isExist = esClient.indices().exists(indexRequest, RequestOptions.DEFAULT)
    if (!isExist) return null
    // 从es中查询相关索引
    val request: SearchRequest = new SearchRequest(indexName)
    val sourceBuilder: SearchSourceBuilder = new SearchSourceBuilder()
    sourceBuilder.fetchSource(fieldName, null).size(1000000)
    request.source(sourceBuilder)
    val response: SearchResponse = esClient.search(request, RequestOptions.DEFAULT)
    // 从索引中获取字段添加到列表中
    val hits: Array[SearchHit] = response.getHits.getHits
    val list: ListBuffer[String] = new ListBuffer[String]
    for (hit <- hits) {
      val map: Map[String, AnyRef] = hit.getSourceAsMap
      val value = map.get(fieldName)
      list.append(value.toString)
    }
    list.toList
  }
}
