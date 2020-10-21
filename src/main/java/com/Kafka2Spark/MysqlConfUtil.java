package com.Kafka2Spark;


import com.Kafka2Spark.dao.ConfigurationManager;
import com.Kafka2Spark.dao.Constants;
import com.mysql.jdbc.Statement;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class MysqlConfUtil {

    private static final String DBDRIVER = "com.mysql.jdbc.Driver";									//驱动程序名
    private static final String DBURL = ConfigurationManager.getProperty(Constants.MYSQL_URL);					//URL指向要访问的数据库名mydata
    private static final String DBUSER = ConfigurationManager.getProperty(Constants.MYSQL_USR);														//MySQL配置时的用户名
    private static final String DBPASSWORD = ConfigurationManager.getProperty(Constants.ORA_PWD); 												//MySQL配置时的密码

    public static Connection getConnection(){
        Connection conn = null;													//声明一个连接对象
        try {
            Class.forName(DBDRIVER);											//注册驱动
            conn = DriverManager.getConnection(DBURL,DBUSER,DBPASSWORD);		//获得连接对象
        } catch (ClassNotFoundException e) {									//捕获驱动类无法找到异常
            e.printStackTrace();
        } catch (SQLException e) {												//捕获SQL异常
            e.printStackTrace();
        }
        return conn;
    }
    public static void close(Connection conn) {//关闭连接对象
        if(conn != null) {				//如果conn连接对象不为空
            try {
                conn.close();			//关闭conn连接对象对象
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    public static void close(PreparedStatement pstmt) {//关闭预处理对象
        if(pstmt != null) {				//如果pstmt预处理对象不为空
            try {
                pstmt.close();			//关闭pstmt预处理对象
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    public static void close(Statement stmt) {//关闭预处理对象
        if(stmt != null) {				//如果pstmt预处理对象不为空
            try {
                stmt.close();			//关闭pstmt预处理对象
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void close(ResultSet rs) {//关闭结果集对象
        if(rs != null) {				//如果rs结果集对象不为null
            try {
                rs.close();				//关闭rs结果集对象
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static Map<TopicPartition, Long> getOffsetMap(String groupid, String[] topic) {
        ArrayList<String> offset = new ArrayList<>();
        Connection con;//声明一个连接对象
        Map<TopicPartition, Long> fromOffsets = new HashMap<>();
        //遍历查询结果集
        try {
            con = getConnection();//1.调用方法返回连接
            if (!con.isClosed())
                System.out.println("Succeeded connecting to the mysql!");
            Statement statement = (Statement) con.createStatement(); //2.创建statement类对象，用来执行SQL语句！！
            String sql = "select * from t_offset where groupid='"+groupid+"' and topic in ('"+String.join("','",topic)+"')";//要执行的SQL语句

            System.out.println(sql);

            ResultSet rs = statement.executeQuery(String.format(sql,groupid,topic));

            while (rs.next()) {
                fromOffsets.put(new TopicPartition(rs.getString("topic"), rs.getInt("partition")), rs.getLong("offset"));

            }
            close(rs);
            close(statement);
            close(con);
            return fromOffsets;
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
        return fromOffsets;
    }


    public static void saveOffsetRanges(OffsetRange o, String groupid) {
        Connection con;//声明一个连接对象
        //遍历查询结果集
        try {
            con = getConnection();//1.调用方法返回连接
            if(!con.isClosed())
                System.out.println("Succeeded connecting to the Mysql!");
            Statement statement = (Statement) con.createStatement(); //2.创建statement类对象，用来执行SQL语句！！
            String sql = "replace into t_offset values ('"+o.topic()+"',"+o.partition()+",'"+groupid+"',"+o.untilOffset()+")";//要执行的SQL语句

            System.out.println(sql);

            if(statement.executeUpdate(String.format(sql))!=0)
                System.out.println("插入成功");
            else
                System.out.println("插入失败");
            close(statement);
            close(con);


        }	catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        String[] stuinfo = {"123456", "小明"};
        //getOffsetMap();
    }
}

