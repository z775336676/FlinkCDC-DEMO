package com.example.caiflinkcdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author 77533
 */
public class MySqlSourceExample {
    private static PreparedStatement pstmt = null;

    private static Connection conn = null;
    private static String url = "jdbc:mysql://127.0.0.1:3306/bi?rewriteBatchedStatements=true&serverTimezone=GMT%2B8&useServerPrepStmts=true&cachePrepStmts=true&characterEncoding=utf8&tinyInt1isBit=false";
    private static String user = "root";
    private static String password = "root";

    public static void main(String[] args) throws Exception {

        String sourceHostname = "172.17.100.192";
        int sourcePort = 30009;
        String sourceUsername = "root";
        String sourcePassword = "Cheyun@De123";
        String sourceDatabase = "matrix-member-demo";
        String sourceTable = "matrix-member-demo.member";

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(sourceHostname)
                .port(sourcePort)
                .databaseList(sourceDatabase) // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList(sourceTable) // 设置捕获的表
                .username(sourceUsername)
                .password(sourcePassword)
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .startupOptions(StartupOptions.latest())//
                .build();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration config = new Configuration();
        config.setInteger(String.valueOf(RestOptions.BIND_PORT), 9091);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

        // 设置 3s 的 checkpoint 间隔
        env.enableCheckpointing(3000);
//        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
//                .setParallelism(4);

        DataStreamSink<String> mySQLSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // 设置 source 节点的并行度为 4
                .setParallelism(1)
                .print().setParallelism(1);

//        //实时同步数据到仓库
//        Connection connection = getConnection();
//        StringBuilder sql = new StringBuilder();
//        sql.append("select * from bi_sell_month");
//        pstmt = connection.prepareStatement(sql.toString());
//        pstmt.execute();
//        pstmt.close();
        // 设置 sink 节点并行度为 1

        env.execute("MySQL Source");


    }

    /**
     * 获取连接
     *
     * @return 连接对象
     */
    public static Connection getConnection() throws SQLException {
        if (conn == null || conn.isClosed()) {
            conn = DriverManager.getConnection(url, user, password);
        }
        return conn;
    }
}

