package com.example.caiflinkcdc.util;

import com.alibaba.fastjson.JSONObject;
import com.example.caiflinkcdc.MySqlSourceExample;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.sql.*;

/**
 * JDBC 工具类
 */
public class DstJDBCUtils {
    private static String url;
    private static String user;
    private static String password;

    private static Connection conn = null;

    static {
        //读取文件，获取值
        try {
            Yaml yaml = new Yaml();
            InputStream in = MySqlSourceExample.class.getClassLoader().getResourceAsStream("base.yaml");
            JSONObject jsonObject = yaml.loadAs(in, JSONObject.class);
            JSONObject dstConf = jsonObject.getJSONObject("dst");
            url = dstConf.getString("url");
            user = dstConf.getString("user");
            password = dstConf.getString("password");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取连接
     * @return 连接对象
     */
    public static Connection getConnection() throws SQLException {
        if(conn==null || conn.isClosed()){
            conn = DriverManager.getConnection(url, user, password);
        }
        return conn;
    }

    /**
     * 释放资源
     * @param rs
     * @param st
     * @param conn
     */
    public static void close(ResultSet rs, Statement st,Connection conn){
        if (rs != null){
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(st != null){
            try {
                st.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
