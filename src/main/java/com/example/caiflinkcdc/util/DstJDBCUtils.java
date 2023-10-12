package com.example.caiflinkcdc.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sun.rowset.JdbcRowSetImpl;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;

import javax.sql.rowset.JdbcRowSet;
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
            InputStream in = DstJDBCUtils.class.getClassLoader().getResourceAsStream("base.yaml");
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
    public synchronized static Connection getConnection() throws SQLException {
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

    public static void useDatabase(String db) throws SQLException {
        Connection conn = getConnection();
        JdbcRowSet jrs = new JdbcRowSetImpl(conn);
        jrs.setCommand("use " + db); //执行选择数据库操作
        jrs.execute();
    }

    /**
     * 查询返回List类型
     * @param sql
     * @param args
     * @return 多条记录的List集合
     */
    public static JSONArray queryForList(String sql, Object... args) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        JSONArray result = new JSONArray();
        try {
            conn = getConnection();
            stmt = conn.prepareStatement(sql);
            if (args != null) {
                for (int i = 0; i < args.length; i++) {
                    stmt.setObject(i + 1, args[i]);
                }
            }
            rs = stmt.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int cols = rsmd.getColumnCount();
            String[] colNames = new String[cols];
            for (int i = 0; i < cols; i++) {
                colNames[i] = rsmd.getColumnName(i + 1);
            }
            while (rs.next()) {
                JSONObject row = new JSONObject();
                for (int i = 0; i < cols; i++) {
                    row.put(colNames[i], rs.getObject(i + 1));
                }
                result.add(row);
            }
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    /**
     * 查询返回Map类型 当确定返回结果唯一时调用
     * @param sql
     * @param args
     * @return 一条记录的Map
     */
    public static JSONObject queryForMap(String sql, Object... args) throws SQLException {
        JSONArray result = queryForList(sql, args);
        JSONObject row = null;
        if (result.size() == 1) {
            row = result.getJSONObject(0);
            return row;
        } else {
            return null;
        }
    }

}
