package edu.lyw.publisherrealtime.mapper.impl;

import edu.lyw.publisherrealtime.bean.NameValue;
import edu.lyw.publisherrealtime.mapper.PublisherMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
@Slf4j
@Repository
public class PublisherMapperImpl implements PublisherMapper {

    @Autowired
    RestHighLevelClient esClient;
    private String dauIndexNamePrefix = "gmall_dau_info_";
    private String orderIndexNamePrefix = "gmall_order_wide_";

    @Override
    public Map<String, Object> searchDetailByItem(String date, String itemName, Integer pageNo, Integer pageSize) {
        String indexName = orderIndexNamePrefix + date;
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(new String[]{"create_time","order_price","province_name","sku_name"," sku_num","total_amount","user_age","user_gender"}, null);

        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("sku_name", itemName).operator(Operator.AND);

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("sku_name");

        searchSourceBuilder.highlighter(highlightBuilder);
        searchSourceBuilder.query(matchQueryBuilder);
        searchSourceBuilder.from(pageNo);
        searchSourceBuilder.size(pageSize);

        searchRequest.source(searchSourceBuilder);
        HashMap<String, Object> results = new HashMap<String, Object>();
        ArrayList<Map<String, Object>> maps = new ArrayList<>();
        try {
            SearchResponse response = esClient.search(searchRequest, RequestOptions.DEFAULT);
            long total = response.getHits().getTotalHits().value;
            SearchHit[] hits = response.getHits().getHits();
            for (SearchHit hit : hits) {
                Map<String, Object> sourceMap = hit.getSourceAsMap();
                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                HighlightField skuName = highlightFields.get("sku_name");
                String highLightSkuName = skuName.getFragments()[0].toString();
                sourceMap.put("sku_name", highLightSkuName);
                maps.add(sourceMap);
            }
            results.put("total", total);
            results.put("detail", maps);
            return results;
        } catch (ElasticsearchStatusException ese) {
            if (ese.status() == RestStatus.NOT_FOUND) {
                log.warn(indexName + "不存在");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("ES查询失败");
        }
        return results;
    }

    @Override
    public List<NameValue> searchStatsByItem(String itemName, String date, String t) {
        String indexName = orderIndexNamePrefix + date;
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("sku_name", itemName).operator(Operator.AND);
        searchSourceBuilder.query(matchQueryBuilder);

        SumAggregationBuilder sumAggregationBuilder = AggregationBuilders.sum("totalamount").field("split_total_amount");
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("groupby" + t).field(t).size(100);
        termsAggregationBuilder.subAggregation(sumAggregationBuilder);
        searchSourceBuilder.aggregation(termsAggregationBuilder);

        searchRequest.source(searchSourceBuilder);
        ArrayList<NameValue> results = new ArrayList<>();
        try {
            SearchResponse response = esClient.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = response.getAggregations();
            ParsedTerms parsedTerms = aggregations.get("groupby" + t);
            List<? extends Terms.Bucket> buckets = parsedTerms.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                String key = bucket.getKeyAsString();
                Aggregations bucketAggregation = bucket.getAggregations();
                ParsedSum parsedSum = bucketAggregation.get("totalamount");
                double totalAmount = parsedSum.getValue();
                results.add(new NameValue(key, totalAmount));
            }
            return results;
        } catch (ElasticsearchStatusException ese) {
            if (ese.status() == RestStatus.NOT_FOUND) {
                log.warn(indexName + "不存在");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("ES查询失败");
        }
        return results;
    }

    @Override
    public Map<String, Object> searchDau(String td) {
        System.out.println("mapper...");
        HashMap<String, Object> results = new HashMap<>();
        // 总数据
        results.put("dauTotal", searchTotalDau(td));
        // 今日数据
        Map<String, Long> dauTd = searchDauHr(td);
        results.put("dauTd", dauTd);
        // 昨日数据
        LocalDate today = LocalDate.parse(td);
        LocalDate yd = today.minusDays(1);
        Map<String, Long> dauYd = searchDauHr(yd.toString());
        results.put("dauYd", dauYd);

        return results;
    }
    /*
     * 查询总量
     */
    public long searchTotalDau(String td) {
        String indexName = dauIndexNamePrefix + td;
        SearchRequest sq = new SearchRequest(indexName);
        SearchSourceBuilder ssBuilder = new SearchSourceBuilder();
        ssBuilder.size(0);
        try {
            SearchResponse response = esClient.search(sq, RequestOptions.DEFAULT);
            return response.getHits().getTotalHits().value;
        } catch (ElasticsearchStatusException ese) {
          if (ese.status() == RestStatus.NOT_FOUND) {
              log.warn(indexName + "不存在");
          }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("ES查询失败");
        }
        return 0L;
    }

    /*
    * 分时明细
    * */
    public Map<String, Long> searchDauHr(String td) {
        String indexName = dauIndexNamePrefix + td;
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder ssbuilder = new SearchSourceBuilder();
        ssbuilder.size(0);
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("groupbyhr").field("hr").size(24);
        ssbuilder.aggregation(termsAggregationBuilder);
        searchRequest.source(ssbuilder);
        HashMap<String, Long> dauHr = new HashMap<>();
        try {
            SearchResponse response = esClient.search(searchRequest, RequestOptions.DEFAULT);
            ParsedTerms parsedTerms = response.getAggregations().get("groupbyhr");
            List<? extends Terms.Bucket> buckets = parsedTerms.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                dauHr.put(bucket.getKeyAsString(), bucket.getDocCount());
            }
            return dauHr;
        } catch (ElasticsearchStatusException ese) {
            if (ese.status() == RestStatus.NOT_FOUND) {
                log.warn(indexName + "不存在");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("ES查询失败");
        }
        return dauHr;
    }
}
