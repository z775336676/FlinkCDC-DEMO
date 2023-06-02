package com.example.caiflinkcdc.sink;

import com.alibaba.fastjson.JSONObject;
import com.example.caiflinkcdc.util.DstJDBCUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * sink到mysql库（实时）
 */
public class JdbcSink extends RichSinkFunction<JSONObject> {

    Connection connection = null;
    PreparedStatement pstmt = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = DstJDBCUtils.getConnection();
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        StringBuilder sql = new StringBuilder();
        //操作类型
        String op = value.getString("op");
        //数据库名
        String db = value.getString("db");
        //表名
        String tb = value.getString("tb");
        JSONObject after = value.getJSONObject("after");
        JSONObject before = value.getJSONObject("before");
        if (after != null) {
            StringBuilder columns = new StringBuilder();
            StringBuilder values = new StringBuilder();
            for (String s : after.keySet()) {
                columns.append(s).append(",");
                values.append("'").append(after.getString(s)).append("',");
            }
            //去掉最后一个逗号
            columns.deleteCharAt(columns.length() - 1);
            values.deleteCharAt(values.length() - 1);

            sql.append("replace into ")/*.append(db).append(".")*/.append(tb).append("(").append(columns)
                    .append(") values (").append(values).append(")");
            System.out.println(sql);
            pstmt = connection.prepareStatement(sql.toString());
            pstmt.execute();
            pstmt.close();
        } else if (op.equals("delete")) {
            sql.append("delete from ").append(tb).append(" where ");
            for (String s : before.keySet()) {
                sql.append(s).append("=");
                sql.append("'").append(before.getString(s)).append("' and ");
            }
            //去掉最后一个and
            sql.delete(sql.length() - 4, sql.length());
            System.out.println(sql);
            pstmt = connection.prepareStatement(sql.toString());
            pstmt.execute();
            pstmt.close();
        }


    }

    @Override
    public void close() throws Exception {
        if (pstmt != null) {
            pstmt.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }
}
