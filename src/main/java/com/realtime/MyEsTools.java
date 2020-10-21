package com.realtime;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.Kafka2Spark.dao.ConfigurationManager;
import com.Kafka2Spark.dao.Constants;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class MyEsTools {

    /**
     * 查看ES路径:elasticsearch-7.5.1\config\elasticsearch.yml cluster.name
     */
    private static final String CLUSTER_NAME = "bigdatatest";

    /**
     * 关闭客户端
     *
     * @param client
     */
    private static void close(RestHighLevelClient client) {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取客户端
     *
     * @return
     */
    private static RestHighLevelClient getClient() {

        String es_servers = ConfigurationManager.getProperty(Constants.ES_CLUSTER_SERVER);
        //String es_servers = "192.168.230.29,192.168.230.30,192.168.230.31";
        String[] es_servers_array = es_servers.split(",");
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(es_servers_array[0], 9200, "http")
                        ,new HttpHost(es_servers_array[1], 9200, "http")
                        //,new HttpHost(es_servers_array[2], 9200, "http")
                        //,new HttpHost("elk-node03", 9200, "http")
                ));
        return client;
    }
    /**
     * match查询
     */
    public static void matchQuery(String index,String column,String fund_account_value) {
        RestHighLevelClient client = getClient();

        // 这里可以不指定索引，也可以指定多个
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(column, fund_account_value);
        searchSourceBuilder.query(matchQueryBuilder);
        //		// 模糊查询
        //		matchQueryBuilder.fuzziness(Fuzziness.AUTO);
        //		// 前缀查询的长度
        //		matchQueryBuilder.prefixLength(3);
        //		// max expansion 选项，用来控制模糊查询
        //		matchQueryBuilder.maxExpansions(10);
        //searchSourceBuilder.from(0);
        //searchSourceBuilder.size(32);
        // 按评分排序
        //searchSourceBuilder.sort("_score");
        SearchResponse search = null;
        try {
            search = client.search(searchRequest, RequestOptions.DEFAULT);
            search.getHits().forEach(line -> {
                JSONObject res1 = JSON.parseObject(line.getSourceAsString());
                String res2 = res1.getString("rowkey");
                System.out.println("************************ " +res2+ "**************************");
            });


        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close(client);
        }
    }

    public static double getById(String id) {

        RestHighLevelClient client = getClient();

        // 这里可以不指定索引，也可以指定多个
        //id = "".equals(id)?"1":id;
        //id = "66001008911";

        GetRequest getRequest = new GetRequest("bi_bwl_acct_totalasset",id);
        GetResponse response = null;
        double s = 0.0;

        try{
            response =  client.get(getRequest, RequestOptions.DEFAULT);
            JSONObject res1 = JSON.parseObject(response.getSourceAsString());

            try{
                String res2 = res1.getString("real_total_asset");
                s =  Double.parseDouble(res2);
            } finally {
                //close(client);
                //return s;
            }
            //String res2 = res1.getString("real_total_asset");
            //System.out.println("************************ " +res1+ "**************************");
            //s =  Double.parseDouble(res2);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close(client);
        }

        return s;

    }

    /**
     * match查询
     * @return
     */
    public static double matchQuerysum(String index,String column,String fund_account_value) {
        RestHighLevelClient client = getClient();

        // 这里可以不指定索引，也可以指定多个
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(column, fund_account_value);
        searchSourceBuilder.query(matchQueryBuilder);
        //		// 模糊查询
        //		matchQueryBuilder.fuzziness(Fuzziness.AUTO);
        //		// 前缀查询的长度
        //		matchQueryBuilder.prefixLength(3);
        //		// max expansion 选项，用来控制模糊查询
        //		matchQueryBuilder.maxExpansions(10);
        //searchSourceBuilder.from(0);
        //searchSourceBuilder.size(32);
        // 按评分排序
        //searchSourceBuilder.sort("_score");
        SearchResponse search = null;
        List<Map<String,Object>> list = new ArrayList<>();
        double sum = 0.0;
        try {
            search = client.search(searchRequest, RequestOptions.DEFAULT);
            search.getHits().forEach(line -> {
                JSONObject res1 = JSON.parseObject(line.getSourceAsString());

                //System.out.println("************************ " +res2+ "**************************");
                if(res1.getString("money_type").equals("0")){
                    String res2 = res1.getString("occur_balance");
                    Map<String,Object> map1 = new HashMap<String,Object>();
                    map1.put("agg", Double.parseDouble(res2));
                    list.add(map1);
                }
            });

            sum  = list.stream().parallel().collect(Collectors.summarizingDouble(x->((Double) x.get("agg")))).getSum();

            //collect(Collectors.summarizingDouble(x->((Double) x.get("agg"))));
            return sum;

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close(client);
        }
        return 0.0;
    }
    /**
     * 条件删除
     */
    public static long deleteQuery() {
        RestHighLevelClient client = getClient();
        //参数为索引名，可以不指定，可以一个，可以多个
        DeleteByQueryRequest request = new DeleteByQueryRequest("hockey");
        // 更新时版本冲突
        request.setConflicts("proceed");
        // 设置查询条件，第一个参数是字段名，第二个参数是字段的值
        request.setQuery(new TermQueryBuilder("first", "sam"));
        // 更新最大文档数
        request.setSize(10);
        // 批次大小
        request.setBatchSize(1000);
        // 并行
        request.setSlices(2);
        // 使用滚动参数来控制“搜索上下文”存活的时间
        request.setScroll(TimeValue.timeValueMinutes(10));
        // 超时
        request.setTimeout(TimeValue.timeValueMinutes(2));
        // 刷新索引
        request.setRefresh(true);
        try {
            BulkByScrollResponse response = client.deleteByQuery(request, RequestOptions.DEFAULT);
            return response.getStatus().getUpdated();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return -1;
    }


    public static double aggregation(String index,String type,String fund_account_value) {
        RestHighLevelClient client = getClient();

        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder field = AggregationBuilders.terms("termName").field(type);
        SumAggregationBuilder sumAggregationBuilder = AggregationBuilders.sum("countName").field("occur_balance");
        TermsAggregationBuilder termsAggregationBuilder = field.subAggregation(sumAggregationBuilder);
        searchSourceBuilder.aggregation(termsAggregationBuilder).query();
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = null;
        try {
            search = client.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = search.getAggregations();
            //System.out.println(aggregations.asMap().values());

            //下面获得聚合查询
            for(Aggregation a:aggregations){
                Terms terms = (Terms) a;
                for(Terms.Bucket bucket:terms.getBuckets()){
                    //System.out.println("key is "+bucket.getKeyAsString());
                    //这里是elasticsearch7的重大改动！！！！必须强转。
                    Sum sum = (Sum) bucket.getAggregations().asMap().get("countName");
                    double value = sum.getValue();
                    //System.out.println("value is "+value);
                    if(fund_account_value.equals(bucket.getKeyAsString())){
                        return value;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return 0.0;
    }

    /**
     * matchAll查询
     */
    public static void matchAllQuery(String index) {
        RestHighLevelClient client = getClient();

        // 这里可以不指定索引，也可以指定多个
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        // 从第一条开始,包括第一条
        //searchSourceBuilder.from(0);
        // 查询30条
        //searchSourceBuilder.size(3);
        // 先按year倒排
        //searchSourceBuilder.sort(SortBuilders.fieldSort("year").order(SortOrder.DESC));
        // 再按id正排
        //searchSourceBuilder.sort("id");
        SearchResponse search = null;
        try {
            search = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        search.getHits().forEach(System.out::println);
        close(client);
    }




    /**
     * Description: 批量插入数据
     *
     * @param list  带插入列表
     * @author fanxb
     * @date 2019/7/24 17:38
     */
    public static void insertBatch(List<Map<String, Object>> list) {
        RestHighLevelClient client = getClient();
        BulkRequest request = new BulkRequest();
        SimpleDateFormat d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();

        list.forEach(p -> {
            Map<String,Object> result = new HashMap<>();
            result.put("position_str",p.get("position_str").toString());
            result.put("money_type",p.get("money_type").toString());
            result.put("fund_account",p.get("fund_account").toString());
            result.put("time_load",p.get("time_load").toString());
            result.put("biz_dt",d.format(date));
            request.add(new IndexRequest(p.get("index").toString()).id(p.get("position_str").toString())
                    .source(result, XContentType.JSON));

        });
        try {
            client.bulk(request, RequestOptions.DEFAULT);
            System.out.println("插入es成功");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        close(client);
    }

    public static void main( String[] args ) throws IOException {

        String operate = "search";
        double s = matchQuerysum("hs_asset_fundjour","fund_account","66001000186");
        System.out.println("value is "+s);
        getById("100000010");

        if ("addOrUpdateDocument".equals(operate)) {

            Long count = 1000L;
            List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

            for (int i = 0; i < count; i++) {
                Map<String, Object> ret = new HashMap<String, Object>();
                ret.put("rowkey", "rowkey" + i);
                ret.put("moneytype", '0');
                ret.put("fund_account", "f001"+i);
                result.add(ret);
            }

            // bulk success
            // getInstance().bulkIndexDocument("fundjour", "fundjour",result);
        } else if ("queryAll".equals(operate)) {
            // ..."totalHits":{"value":3,"relation":"EQUAL_TO"},"maxScore":1.0}
            matchAllQuery("fundjour");
        } else if ("search".equals(operate)) {
            // 查询全部
            matchQuery("fundjour","fund_account","f0003");

        }

    }

}
