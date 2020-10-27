package com.realtime;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import com.Kafka2Spark.dao.ConfigurationManager;
import com.Kafka2Spark.dao.Constants;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.realtime.MyEsTools;

public class StockReal2ES2ORA {

    public static void main(String[] args) throws InterruptedException {

        //System.setProperty("java.security.auth.login.config", ConfigurationManager.getProperty(Constants.SECURITY_AUTH_LOGIN_CONFIG));
        //System.setProperty("java.security.krb5.conf", ConfigurationManager.getProperty(Constants.SECURITY_KRB5_CONFIG));

        String myMaster = ConfigurationManager.getProperty(Constants.SPARK_MASTER);

        // 构建SparkStreaming上下
        SparkConf conf = new SparkConf().setAppName("StockReals2ES").setMaster(myMaster);

        // 每隔5秒钟，sparkStreaming作业就会收集5秒内的数据源接收过来的数
        String time_interval = ConfigurationManager.getProperty(Constants.TIME_INTERVAL);
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(Integer.parseInt(time_interval)));

        //jssc.checkpoint("D:\\streaming_checkpoint");

        //创建map类型以传
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", ConfigurationManager.getProperty(Constants.KAFKA_BOOTSTRAP_SERVERS));
        kafkaParams.put("group.id", "realtime_StockReals2ES_consumer");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);

        //kerberos安全认证
        kafkaParams.put("security.protocol", "SASL_PLAINTEXT");
        //kafkaParams.put("sasl.mechanism", "GSSAPI");
        kafkaParams.put("sasl.kerberos.service.name", "kafka");


        String topics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
        String[] topicArr = topics.split(",");

        Collection<String> topicSet = Arrays.asList(topicArr);
        //HashSet topicSet = new HashSet<String>();
        //topicSet.add(ConfigurationManager.getProperty(Constants.KAFKA_TOPICS));
        kafkaParams.put("auto.offset.reset", "latest");
        //kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);
        Map<TopicPartition, Long> maptopic = new HashMap<>();


        try {
            // 获取kafka的数
            final JavaInputDStream<ConsumerRecord<String, String>> Dstream =
                    KafkaUtils.createDirectStream(
                            jssc,
                            LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.<String, String>Subscribe(topicSet, kafkaParams)
                    );

            String[] tablename_rule1 = {"STOCKREAL","CRDTSTOCKREAL","SECUMREAL"};
            String[] tablename_rule2 = {"FUNDJOUR",""};
            String[] tablename_rule3 = {"FUNDACCOUNTJOUR"};
            String[] tablename_rule4 = {"BANKTRANSFER"};
            String[] entrust_bs_rule = {"1","2"};//委托种类
            String[] exchange_type_rule = {"1","2"};//交易类别
            String[] branch_no_rule = {"8888","9800","9900"};//自营机构
            String[] real_status_rule = {"0","4"};//处理标志
            String[] real_type_rule1 = {"0"};//成交类型
            String[] real_type_rule2 = {"6","7","8","9"};//成交类型
            String[] stock_type_rule = {"0","1","d","c","h","e","g","D","L","6","T","p","q"};//证券类别
            String[] operType_rule = {"I"};
            String[] operType_rule2 = {"I","U"};
            //operType  D：delete;I:insert;U:update:DT:truncate;
            String[] trans_type_rule = {"01","02"};//转账类型
            String[] money_type_rule = {"0","1"};//货币代码
            String[] bktrans_status_rule = {"2"};//转账状
            String[] asset_prop_rule = {"0"};//账户属
            String[] business_flag_rule = {"2041","2042","2141","2142"};//业务品种


            //逐一处理每条消息
            Dstream.foreachRDD(rdd-> {
                OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                //System.out.print("-----------------------" + offsetRanges + "-----------------------------------\n");

                rdd.foreachPartition(partitions -> {

                    List<Map<String, Object>> list_stockreal = new ArrayList<Map<String, Object>>();
                    List<Date> dates = new ArrayList<Date>();

                    partitions.forEachRemaining(line -> {
                        //System.out.print("***************************" + line.value() + "***************************\n");
                        //List<String> list2 = new ArrayList<>();
                        //todo 获取到kafka的每条数 进行操作
                        //System.out.print("***************************" + line.value() + "***************************");
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        String newline = line.value().replace("\n|\r","");
                        boolean flag = false;
                        flag=MyUtils.isJson(newline);
                        JSONObject res1 = null;
                        String tablename = null;
                        String timeload = null;
                        String operType = null;

                        if(flag){
                            res1 = JSON.parseObject(newline);
                            tablename = res1.getString("Tablename");
                            timeload = res1.getString("timeload");
                            operType = res1.getString("operType");
                            try{
                                dates.add(format.parse(timeload));
                            }catch (Exception e){
                                e.printStackTrace();
                            }

                            JSONObject res2 = null;

                            if(res1.containsKey("columnInfo")){
                                res2 = JSON.parseObject(res1.getString("columnInfo"));
                            } else if(res1.containsKey("columns")){
                                res2 = JSON.parseObject(res1.getString("columns"));
                            } else {
                                flag = false;
                            }


                            if (Arrays.asList(tablename_rule2).contains(tablename)
                                    &Arrays.asList(operType_rule2).contains(operType)
                                    &flag){

                                String fund_account = res2.getString("FUND_ACCOUNT");
                                String money_type = res2.getString("MONEY_TYPE");
                                String business_flag = res2.getString("BUSINESS_FLAG");
                                String occur_balance = res2.getString("OCCUR_BALANCE");
                                String rowkey = res2.getString("POSITION_STR");
                                if(Arrays.asList(money_type_rule).contains(money_type)
                                        &Arrays.asList(business_flag_rule).contains(business_flag)) {

                                    Map<String, Object> map1= new HashMap<String, Object>();
                                    map1.put("position_str", rowkey);
                                    map1.put("money_type", money_type);
                                    map1.put("fund_account", fund_account);
                                    map1.put("columnValue",occur_balance);
                                    map1.put("time_load",timeload);
                                    map1.put("index","hs_asset_fundjour");
                                    map1.put("init_date",res2.getString("INIT_DATE"));

                                    list_stockreal.add(map1);
                                }

                            }else if(Arrays.asList(tablename_rule1).contains(tablename)
                                    &Arrays.asList(operType_rule2).contains(operType)
                                    &flag){
                                String fund_account = res2.getString("FUND_ACCOUNT");
                                String money_type = res2.getString("MONEY_TYPE");
                                String occur_balance = "0";
                                String rowkey = res2.getString("POSITION_STR");

                                Map<String, Object> map1= new HashMap<String, Object>();
                                map1.put("position_str", rowkey);
                                map1.put("money_type", money_type);
                                map1.put("fund_account", fund_account);
                                map1.put("columnValue",occur_balance);
                                map1.put("time_load",timeload);
                                map1.put("init_date",res2.getString("INIT_DATE"));

                                if(tablename.equals("STOCKREAL")){
                                    map1.put("index","hs_secu_stockreal");
                                } else if (tablename.equals("CRDTSTOCKREAL")){
                                    map1.put("index","hs_crdt_crdtstockreal");
                                } else if (tablename.equals("SECUMREAL")){
                                    map1.put("index","hs_prod_secumreal");
                                };

                                list_stockreal.add(map1);


                            }else {
                                //System.out.println("************************ not match:" + res1.values() + "**************************");
                            }
                        }
                    });

                    try{
                        //insertMany(hbtable,list);
                        if(list_stockreal.size()>0) {

                            //insertDetail(list_fundjour, dates,"realtime_bank_transfer");
                            //insertDetail2Hbase(list_fundjour,"RL_ARM:BANKTRANSFER_FUNDJOUR");

                            MyEsTools.insertBatch(list_stockreal);
                            //MyUtils.insertDetail2Oracle(list_stockreal, dates,"hs_asset_fundjour");
                            //System.out.println(list_stockreal);

                        } else{
                            System.out.println("没有符合条件的数");
                        }
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                });
                ((CanCommitOffsets) Dstream.inputDStream()).commitAsync(offsetRanges);
            });

            jssc.start();
            jssc.awaitTermination();
            jssc.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
