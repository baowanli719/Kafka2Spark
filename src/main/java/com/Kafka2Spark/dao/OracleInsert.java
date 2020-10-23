package com.Kafka2Spark.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import com.Kafka2Spark.dao.OracleConn;

public class OracleInsert {
    public static void main(String[] args) {
        Connection conn=null;
        Statement st=null;
        conn = OracleConn.getconn();
        try {
            //获得连接
            st=conn.createStatement();
            //创建插入的sql语句
            String sql="insert into fundreal_guarantee_balance (BIZ_DT,BALANCE) values('12',12.12)";
            //返回一个进行此操作的结果，要么成功，要么失败，如果返回的结果>0就是成功，反之失败
            int result=st.executeUpdate(sql);
            if(result>0) {
                System.out.println("添加成功");
            }
            else {
                System.out.println("添加失败");
            }

        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
