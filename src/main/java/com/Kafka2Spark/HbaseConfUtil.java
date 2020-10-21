package com.Kafka2Spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

public class HbaseConfUtil {
    private String zookeeperQuorum = "cdh4";
    private String clientPort = "2181";
    private String znodeParent = "/hbase";
    private String kerberosConfPath = "D:\\myconf\\krb5.conf";
    private String principal = "hbase/cdh3@HD.COM";
    private String keytabPath = "D:\\myconf\\hbase.keytab";

    public Configuration getHbaseConf() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zookeeperQuorum);
        conf.set("hbase.zookeeper.property.clientPort", clientPort);
        conf.set("zookeeper.znode.parent", znodeParent);
        //conf.setInt("hbase.rpc.timeout", 20000);
        //conf.setInt("hbase.client.operation.timeout", 30000);
        //conf.setInt("hbase.client.scanner.timeout.period", 200000);
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hbase.security.authentication", "kerberos");
        //conf.set("kerberos.principal" , "hbase/cdh3@HD.COM" );
        //conf.set("hbase.master.kerberos.principal","hbase/cdh3@HD.COM");
        conf.set("hbase.regionserver.kerberos.principal","hbase/cdh3@HD.COM");

        File corefile=new File("D:\\myconf\\core-site.xml"); //根据路径创建File类对象--这里路径即使错误也不会报错，因为只是产生File对象，还并未与计算机文件读写有关联
        File hbasefile=new File("D:\\myconf\\hbase-site.xml"); //根据路径创建File类对象--这里路径即使错误也不会报错，因为只是产生File对象，还并未与计算机文件读写有关联
        File hdfsfile=new File("D:\\myconf\\hdfs-site.xml"); //根据路径创建File类对象--这里路径即使错误也不会报错，因为只是产生File对象，还并未与计算机文件读写有关联

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


        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        System.setProperty("java.security.krb5.conf", kerberosConfPath);
        return conf;
    }

    public synchronized Connection getConnection() throws Exception {
        Connection connection = null;
        Configuration conf = getHbaseConf();
        // 使用票据登陆Kerberos
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(principal, keytabPath);

        connection = ConnectionFactory.createConnection(conf);
        return connection;
    }

    public static void main(String[] args) throws Exception {
        HbaseConfUtil hbaseConfUtil = new HbaseConfUtil();
        Connection connection = null;
        Admin admin = null;
        try {

            connection = hbaseConfUtil.getConnection();
            admin = connection.getAdmin();

            String tableName = "bwl";

            if (!admin.isTableAvailable(TableName.valueOf(tableName))) {
                HTableDescriptor hbaseTable = new HTableDescriptor(TableName.valueOf(tableName));
                hbaseTable.addFamily(new HColumnDescriptor("name"));
                hbaseTable.addFamily(new HColumnDescriptor("contact_info"));
                hbaseTable.addFamily(new HColumnDescriptor("personal_info"));
                admin.createTable(hbaseTable);
            }
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
