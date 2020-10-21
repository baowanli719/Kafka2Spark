package com.Kafka2Spark;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.security.UserGroupInformation;
import java.util.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTest {


    private static Connection connection =null;
    private static Admin admin =null;

    static {
        System.setProperty("java.security.krb5.conf", "D:\\myconf\\krb5.conf");
        System.setProperty("sun.security.krb5.debug", "true");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

        final Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "cdh1,cdh2,cdh3,cdh4");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hbase.security.authentication", "kerberos");
        conf.set("zookeeper.znode.parent", "/hbase-secure");
        //conf.set("hbase.master.kerberos.principal", "hbase@HD.COM");
        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI("hbase/cdh3@HD.COM", "D:\\myconf\\hbase.keytab");
            UserGroupInformation.setLoginUser(ugi);
            HBaseAdmin.checkHBaseAvailable(conf);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createTable(String tablename,String... cf1) throws Exception{
        //获取admin对象
        Admin admin = connection.getAdmin();
        //创建tablename对象描述表的名称信息
        TableName tname = TableName.valueOf(tablename);//bd17:mytable
        //创建HTableDescriptor对象，描述表信息
        HTableDescriptor tDescriptor = new HTableDescriptor(tname);
        //判断是否表已存在
        if(admin.tableExists(tname)){
            System.out.println("表"+tablename+"已存在");
            return;
        }
        //添加表列簇信息
        for(String cf:cf1){
            HColumnDescriptor famliy = new HColumnDescriptor(cf);
            tDescriptor.addFamily(famliy);
        }
        //调用admin的createtable方法创建表
        admin.createTable(tDescriptor);
        System.out.println("表"+tablename+"创建成功");
    }
    //删除表
    public void deleteTable(String tablename) throws Exception{
        Admin admin = connection.getAdmin();
        TableName tName = TableName.valueOf(tablename);
        if(admin.tableExists(tName)){
            admin.disableTable(tName);
            admin.deleteTable(tName);
            System.out.println("删除表"+tablename+"成功！");
        }else{
            System.out.println("表"+tablename+"不存在。");
        }
    }

    public void query(){
        HTable table = null;
        ResultScanner scan = null;
        try{
            //Admin admin = connection.getAdmin();
            table = (HTable)connection.getTable(TableName.valueOf("yangsy"));

            scan = table.getScanner(new Scan());

            for(Result rs : scan){
                System.out.println("rowkey:" + new String(rs.getRow()));

                for(Cell cell : rs.rawCells()){
                    System.out.println("column:" + new String(CellUtil.cloneFamily(cell)));

                    System.out.println("columnQualifier:"+new String(CellUtil.cloneQualifier(cell)));

                    System.out.println("columnValue:" + new String(CellUtil.cloneValue(cell)));

                    System.out.println("----------------------------");
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            try {
                table.close();
                if(null != connection) {
                    connection.close();
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public void queryByRowKey(){
        ResultScanner scann = null;
        HTable table = null;
        try {
            table = (HTable)connection.getTable(TableName.valueOf("yangsy"));

            Result rs = table.get(new Get("1445320222118".getBytes()));
            System.out.println("yangsy the value of rokey:1445320222118");
            for(Cell cell : rs.rawCells()){
                System.out.println("family" + new String(CellUtil.cloneFamily(cell)));
                System.out.println("value:"+new String(CellUtil.cloneValue(cell)));
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
    //新增数据到表里面Put
    public void putData(String table_name) throws Exception{
        TableName tableName = TableName.valueOf(table_name);
        Table table = connection.getTable(tableName);
        Random random = new Random();
        List<Put> batPut = new ArrayList<Put>();
        for(int i=0;i<10;i++){
            //构建put的参数是rowkey   rowkey_i (Bytes工具类，各种java基础数据类型和字节数组之间的相互转换)
            Put put = new Put(Bytes.toBytes("rowkey_"+i));
            put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("username"), Bytes.toBytes("un_"+i));
            put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("age"), Bytes.toBytes(random.nextInt(50)+1));
            put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("birthday"), Bytes.toBytes("20170"+i+"01"));
            put.addColumn(Bytes.toBytes("j"), Bytes.toBytes("phone"), Bytes.toBytes("电话_"+i));
            put.addColumn(Bytes.toBytes("j"), Bytes.toBytes("email"), Bytes.toBytes("email_"+i));
            //单记录put
//            table.put(put);
            batPut.add(put);
        }
        table.put(batPut);
        System.out.println("表插入数据成功！");
    }

    //关闭连接
    public void cleanUp() throws Exception{
        connection.close();
    }
    /**
     *@paramargs
     */
    public static void main(String[] args) {

        HbaseTest hbaseTest = new HbaseTest();
        try{
            hbaseTest.createTable("baowl:test","i","j");
        }catch (Exception e){
            e.printStackTrace();
        }

        try{
            hbaseTest.putData("baowl:test");
        }catch (Exception e){
            e.printStackTrace();
        }
        try{
            hbaseTest.cleanUp();
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}