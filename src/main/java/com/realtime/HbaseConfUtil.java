package com.realtime;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import com.Kafka2Spark.dao.ConfigurationManager;
import com.Kafka2Spark.dao.Constants;

import java.util.*;

public class HbaseConfUtil {

    private static String zookeeperQuorum = ConfigurationManager.getProperty(Constants.HBASE_ZOOKEEPER_QUORUM);
    private static String clientPort = "2181";//ConfigurationManager.getProperty(Constants.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT);
    //private String znodeParent = "/hbase";
    private static String principal =  ConfigurationManager.getProperty(Constants.HBASE_REGIONSERVER_PRINCIPAL);

    //private String kerberosConfPath = "/root/gbicc/config/krb5.conf";
    //private String keytabPath = "/opt/udata/realtime.keytab";

    private static String kerberosConfPath = ConfigurationManager.getProperty(Constants.SECURITY_KRB5_CONFIG);
    private static String keytabPath = ConfigurationManager.getProperty(Constants.HBASE_REGIONSERVER_KEYTAB_FILE);
    //System.setProperty("java.security.auth.login.config", "/root/gbicc/config/jaas.conf");
    //System.setProperty("java.security.krb5.conf", "/root/gbicc/config/krb5.conf");

    private static Connection connection = null;


    public Configuration getHbaseConf() {


        Configuration conf = HBaseConfiguration.create();

        //File corefile=new File("/root/gbicc/config/core-site.xml"); //根据路径创建File类对象--这里路径即使错误也不会报错，因为只是产生File对象，还并未与计算机文件读写有关联
        //File hbasefile=new File("/root/gbicc/config/hbase-site.xml"); //根据路径创建File类对象--这里路径即使错误也不会报错，因为只是产生File对象，还并未与计算机文件读写有关联
        //File hdfsfile=new File("/root/gbicc/config/hdfs-site.xml"); //根据路径创建File类对象--这里路径即使错误也不会报错，因为只是产生File对象，还并未与计算机文件读写有关联

        File corefile=new File(ConfigurationManager.getProperty(Constants.CORE_SITE_PATH)); //根据路径创建File类对象--这里路径即使错误也不会报错，因为只是产生File对象，还并未与计算机文件读写有关联
        File hbasefile=new File(ConfigurationManager.getProperty(Constants.HBASE_SITE_PATH)); //根据路径创建File类对象--这里路径即使错误也不会报错，因为只是产生File对象，还并未与计算机文件读写有关联
        File hdfsfile=new File(ConfigurationManager.getProperty(Constants.HDFS_SITE_PATH)); //根据路径创建File类对象--这里路径即使错误也不会报错，因为只是产生File对象，还并未与计算机文件读写有关联

        try
        {
            FileInputStream coreStream=new FileInputStream(corefile);//与根据File类对象的所代表的实际文件建立链接创建fileInputStream对象
            conf.addResource(coreStream);
        }
        catch (FileNotFoundException e)
        {

            System.out.println("文件不存在或者文件不可读或者文件是目录");
        }

        try
        {
            FileInputStream hbaseStream=new FileInputStream(hbasefile);//与根据File类对象的所代表的实际文件建立链接创建fileInputStream对象
            conf.addResource(hbaseStream);
        }
        catch (FileNotFoundException e)
        {

            System.out.println("文件不存在或者文件不可读或者文件是目录");
        }

        try
        {
            FileInputStream hdfsStream=new FileInputStream(hdfsfile);//与根据File类对象的所代表的实际文件建立链接创建fileInputStream对象
            conf.addResource(hdfsStream);
        }
        catch (FileNotFoundException e)
        {

            System.out.println("文件不存在或者文件不可读或者文件是目录");
        }

        conf.set("hbase.zookeeper.quorum", zookeeperQuorum);
        conf.set("hbase.zookeeper.property.clientPort", clientPort);
        //conf.set("zookeeper.znode.parent", znodeParent);
        conf.setInt("hbase.rpc.timeout", 20000);
        conf.setInt("hbase.client.operation.timeout", 30000);
        conf.setInt("hbase.client.scanner.timeout.period", 200000);
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hbase.security.authentication", "kerberos");
        //conf.set("kerberos.principal" , "hbase/cdh3@HD.COM" );
        //conf.set("hbase.master.kerberos.principal","hbase/cdh3@HD.COM");
        //conf.set("hbase.regionserver.kerberos.principal","realtime@TESTBIGDATA.COM");


        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        System.setProperty("java.security.krb5.conf", kerberosConfPath);
        return conf;
    }

    public synchronized Connection getConnection() throws Exception {

        Configuration conf = getHbaseConf();
        // 使用票据登陆Kerberos
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(principal, keytabPath);

        connection = ConnectionFactory.createConnection(conf);
        return connection;
    }

    public void queryByRowKey(){
        ResultScanner scann = null;
        HTable table = null;
        try {
            table = (HTable)connection.getTable(TableName.valueOf("yangsy"));

            Result rs = table.get(new Get("1445320222118".getBytes()));
            System.out.println("yangsy the value of rokey:1445320222118");
            for(Cell cell : rs.rawCells()){
                System.out.println("family" + new String(CellUtil.cloneFamily((org.apache.hadoop.hbase.Cell) cell)));
                System.out.println("value:"+new String(CellUtil.cloneValue((org.apache.hadoop.hbase.Cell) cell)));
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally{
            if(null != table){
                try{
                    table.close();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }

    public static void insertMany(TableName tableName, List<Map<String, Object>> list) throws Exception{
        List<Put> puts = new ArrayList<Put>();
        //Table负责跟记录相关的操作如增删改查等

        ResultScanner scann = null;
        HTable table = null;
        try {

            table = (HTable)connection.getTable(tableName);

            if (list != null && list.size() > 0){
                for (Map<String, Object> map: list){
                    Put put = new Put(Bytes.toBytes(map.get("rowKey").toString()));
                    put.addColumn(Bytes.toBytes(map.get("columnFamily").toString()), Bytes.toBytes(map.get("columnName").toString()), Bytes.toBytes(map.get("columnValue").toString()));
                    puts.add(put);
                }
            }
            table.put(puts);
            table.close();
            System.out.println("add data Success!");
        }catch (Exception e){
            e.printStackTrace();
        }finally{
            if(null != table){
                try{
                    table.close();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        HbaseConfUtil hbaseConfUtil = new HbaseConfUtil();
        Connection connection = null;
        Admin admin = null;
        System.out.println("principal:"+principal);
        try {

            connection = hbaseConfUtil.getConnection();
            
            
            /*   admin = connection.getAdmin();

            String tableName = "bwl2";

            if (!admin.isTableAvailable(TableName.valueOf(tableName))) {
                HTableDescriptor hbaseTable = new HTableDescriptor(TableName.valueOf(tableName));
                hbaseTable.addFamily(new HColumnDescriptor("name"));
                hbaseTable.addFamily(new HColumnDescriptor("contact_info"));
                hbaseTable.addFamily(new HColumnDescriptor("personal_info"));
                admin.createTable(hbaseTable);
            }
            
        */

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }

                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }
}

