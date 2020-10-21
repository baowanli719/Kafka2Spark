package com.realtime;

import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

import com.Kafka2Spark.dao.OracleConn;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class MyUtils {

    public static void insertDetail2Hbase(List<Map<String, Object>> list,String tablename) throws Exception{

        HbaseConfUtil hbaseConfUtil = new HbaseConfUtil();
        Connection connection = null;
        TableName tableName = TableName.valueOf(tablename);
        connection = hbaseConfUtil.getConnection();

        try {

            HbaseConfUtil.insertMany(tableName,list);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {

                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }

    public static void insertDetail2Oracle(List<Map<String, Object>> list,List<Date> dates,String tablename) throws Exception{

        Map<String,Double> occur_balance = new HashMap<>();
        Map<String,String> fund_account = new HashMap<>();
        Map<String,String> load_time = new HashMap<>();
        Map<String,String> init_date = new HashMap<>();
        Map<String,String> money_type = new HashMap<>();
        Multimap<String,Object> multimap = ArrayListMultimap.create();
        SimpleDateFormat d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<Map<String, Object>> ulist = new ArrayList<Map<String, Object>>();
        List<Map<String, Object>> rlist = new ArrayList<Map<String, Object>>();

        //System.out.println(list);
        ulist = list.stream().distinct().collect(Collectors.toList());
        //System.out.println(ulist);


        //System.out.println(String sql = "";);
        //System.out.println(d.format(dates.get(0)));
        Date date = new Date();
        //System.out.println(d.format(date));


        java.sql.Connection conn=null;
        Statement exsql=null;
        conn = OracleConn.getconn();
        //获得连接
        exsql=conn.createStatement();

        int ora_insert_result = 0;

        ulist.forEach(p->{

            if(p.get("index").toString().equals(tablename)){
                occur_balance.put(p.get("position_str").toString(),Double.parseDouble(p.get("columnValue").toString()));
                fund_account.put(p.get("position_str").toString(),p.get("fund_account").toString());
                load_time.put(p.get("position_str").toString(),p.get("time_load").toString());
                init_date.put(p.get("position_str").toString(),p.get("init_date").toString());
                money_type.put(p.get("position_str").toString(),p.get("money_type").toString());
            }
        });

        try {

            for(String key : occur_balance.keySet()){
                double value = occur_balance.get(key);
                System.out.println(key+"  "+value);
                String key1 = init_date.get(key);
                String key2 = key;
                String acct = fund_account.get(key);
                String moneytype = money_type.get(key);
                String sql = "";

                sql="MERGE INTO realtime_bank_transfer T1\n" +
                        "USING (SELECT '"+d.format(dates.get(0))+"' AS biz_dt, "+value+" AS balance, '"+d.format(date)+"' as load_time,'"+key1+"' as init_time,'"+key2+"' as attr1,'"+acct+"' as fund_account,'"+moneytype+"' as money_type FROM dual)  T2\n" +
                        "ON ( T1.Attr1=T2.Attr1)\n" +
                        "WHEN MATCHED THEN\n" +
                        "UPDATE SET T1.biz_dt=T2.biz_dt,T1.balance=T2.balance,T1.load_time=T2.load_time,T1.fund_account=T2.fund_account,T1.money_type=T2.money_type\n" +
                        "WHEN NOT MATCHED THEN INSERT VALUES(T2.biz_dt,T2.balance,T2.load_time,T2.init_time,T2.attr1,T2.fund_account,T2.money_type)";
                //System.out.println(sql);

                //返回个进行此操作的结果，要么成功，要么失败，如果返回的结>0就是成功，反之失
                ora_insert_result = exsql.executeUpdate(sql);
                if(ora_insert_result>0) {
                    System.out.println("realtime_bank_transfer 添加成功");
                }
                else {
                    System.out.println("添加失败");
                }
            }


        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


        try {
            if (exsql!=null) exsql.close();
            if (conn!=null) conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static void insertSum(List<Map<String, Object>> list,List<Date> dates,String tablename) throws Exception{

        Map<String,Double> result = new HashMap<>();
        Multimap<String,Object> multimap = ArrayListMultimap.create();
        SimpleDateFormat d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


        list.forEach(p->{
            multimap.put(p.get("columnName").toString(),p.get("columnValue"));
        });
        multimap.keySet().forEach(key->{
            Double totalScore = multimap.get(key).stream().map(p->{
                return Double.parseDouble(p.toString());
            }).reduce(0.0,Double::sum);
            result.put(key,totalScore);
        });
        System.out.println(result);
        System.out.println(d.format(dates.get(0)));
        Date date = new Date();
        System.out.println(d.format(date));


        java.sql.Connection conn=null;
        Statement exsql=null;
        conn = OracleConn.getconn();

        try {
            //获得连接
            exsql=conn.createStatement();

            for(String key : result.keySet()){
                double value = result.get(key);
                System.out.println(key+"  "+value);
                String key1 = key.substring(0,8);
                String key2 = key.substring(8);


                //创建插入的sql语句
                String sql="insert into "+tablename+" (BIZ_DT,BALANCE,LOAD_TIME,INIT_DATE,ATTR1) values('"+d.format(dates.get(0))+"',"+value+",'"+d.format(date)+"',"+"'"+key1+"',"+"'"+key2+"')";
                System.out.println(sql);

                //返回个进行此操作的结果，要么成功，要么失败，如果返回的结>0就是成功，反之失
                int ora_insert_result=exsql.executeUpdate(sql);
                if(ora_insert_result>0) {
                    System.out.println("realtime_trading_amount 添加成功");
                }
                else {
                    System.out.println("添加失败");
                }
            }


        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        try {
            if (exsql!=null) exsql.close();
            if (conn!=null) conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static boolean isJson(String content) {
        if(StringUtils.isEmpty(content)){
            return false;
        }
        boolean isJsonObject = true;
        boolean isJsonArray = true;
        try {
            JSONObject.parseObject(content);
        } catch (Exception e) {
            isJsonObject = false;
        }
        try {
            JSONObject.parseArray(content);
        } catch (Exception e) {
            isJsonArray = false;
        }
        if(!isJsonObject && !isJsonArray){ //不是json格式
            return false;
        }
        return true;
    }

}
