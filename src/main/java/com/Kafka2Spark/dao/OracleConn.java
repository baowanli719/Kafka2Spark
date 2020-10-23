package com.Kafka2Spark.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class OracleConn {
    static String driverClass="oracle.jdbc.driver.OracleDriver"; //oracle的驱动
    static String url=ConfigurationManager.getProperty(Constants.ORA_URL);  //连接oracle路径方式 “”gfs“”是要建立连接的数据库名 1521端口
    static String user=ConfigurationManager.getProperty(Constants.ORA_USR);   //user是数据库的用户名
    static String password=ConfigurationManager.getProperty(Constants.ORA_PWD);  //用户登录密码

    public static Connection getconn() {  //为了方便下面的讲解，这里专门建立了一个用于数据库连接的一个方法
        Connection conn=null;
        try {

            //首先建立驱动
            Class.forName("oracle.jdbc.driver.OracleDriver");

            //驱动成功后进行连接
            conn=DriverManager.getConnection(url, user, password);

            System.out.println("连接成功");
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn; //返回一个连接
    }
}
//自己在测试连接的时候可以不用定义连接方法