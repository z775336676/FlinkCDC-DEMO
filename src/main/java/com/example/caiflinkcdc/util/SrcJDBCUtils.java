package com.example.caiflinkcdc.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sun.rowset.JdbcRowSetImpl;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.rowset.JdbcRowSet;
import java.io.InputStream;
import java.sql.*;

/**
 * JDBC 工具类
 */
public class SrcJDBCUtils {

    private static final Logger log = LoggerFactory.getLogger(SrcJDBCUtils.class);

    private static String host;
    private static String port;
    private static String username;
    private static String password;
    private static String databases;

    private static Connection conn = null;

    private static boolean flag = false;

    public static void init(String src) {
        if (!flag) {
            //读取文件，获取值
            try {
                Yaml yaml = new Yaml();
                InputStream in = SrcJDBCUtils.class.getClassLoader().getResourceAsStream("base.yaml");
                JSONObject jsonObject = yaml.loadAs(in, JSONObject.class);
                JSONObject dstConf = jsonObject.getJSONObject(src);
                host = dstConf.getString("host");
                port = dstConf.getString("port");
                username = dstConf.getString("username");
                password = dstConf.getString("password");
                databases = dstConf.getString("databases");
                flag = true;
            } catch (Exception e) {
                log.info(e.toString());
            }
        }
    }

    /**
     * 获取连接
     *
     * @return 连接对象
     */
    public static Connection getConnection() throws SQLException {
        if (conn == null || conn.isClosed()) {
            String db;
            String[] dbs = databases.split(",");
            if(dbs.length>0){
                db = dbs[0];
            }else{
                db = databases;
            }
            conn = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/"+db+"?rewriteBatchedStatements=true&serverTimezone=GMT%2B8&tinyInt1isBit=false", username, password);
        }
        return conn;
    }

    /**
     * 释放资源
     *
     * @param rs
     * @param st
     * @param conn
     */
    public static void close(ResultSet rs, Statement st, Connection conn) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (st != null) {
            try {
                st.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static String getDDL(String db, String tb) {
        PreparedStatement ps = null;
        Connection conn = null;
        ResultSet rs = null;
        String ddl = null;
        try {
            conn = getConnection();
            JdbcRowSet jrs = new JdbcRowSetImpl(conn);
            jrs.setCommand("use " + db); //执行选择数据库操作
            jrs.execute();
            ps = conn.prepareStatement("SHOW CREATE TABLE " + tb);
            rs = ps.executeQuery();
            while (rs.next()) {
                //System.out.println(rs.getString(1));//第一个参数获取的是tableName
                ddl = rs.getString(2);//第二个参数获取的是表的ddl语句
                String comment = "";
                if(ddl.lastIndexOf("COMMENT=")>0){
                    comment = ddl.substring(ddl.lastIndexOf("COMMENT="));
                }
                ddl = ddl.substring(0, ddl.lastIndexOf(") ENGINE=") + 1) + comment;
                //特殊情况处理
                ddl = ddl.replace("CHARACTER SET utf8mb4", "");
                ddl = ddl.replace("COLLATE utf8mb4_0900_ai_ci", "");
                ddl = ddl.replace("CHARACTER SET gbk", "");
                ddl = ddl.replace("DEFAULT '0000-00-00'","DEFAULT '1900-01-01'");
                ddl = ddl.replace("DEFAULT '0000-00-00 00:00:00'","DEFAULT '1900-01-01 00:00:00'");
            }
        } catch (SQLException e) {
            log.info(e.toString());
        } finally {
            close(rs, ps, conn);
        }
        return ddl;
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
        }  finally {
            try {
                if (rs != null)
                    rs.close();
                if (stmt != null)
                    stmt.close();
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
