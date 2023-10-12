package com.example.caiflinkcdc;

import com.alibaba.fastjson.JSONObject;
import com.example.caiflinkcdc.func.ETLKbossWindowFunction;
import com.example.caiflinkcdc.sink.JdbcSink;
import com.example.caiflinkcdc.sink.MySink;
import com.example.caiflinkcdc.util.JsonDeserializationSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * @author 77533
 */
public class MySqlSourceJSON {

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
        String hdfsPath = "hdfs://localhost:9000/hdfs-api/test";

        MySqlSource<JSONObject> mySqlSource = MySqlSource.<JSONObject>builder()
                .hostname(sourceHostname)
                .port(sourcePort)
                .databaseList(sourceDatabase) // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList(sourceTable) // 设置捕获的表
                .username(sourceUsername)
                .password(sourcePassword)
                .deserializer(new JsonDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .startupOptions(StartupOptions.latest())//配置增量同步
                .build();

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration config = new Configuration();
        config.setInteger(String.valueOf(RestOptions.BIND_PORT), 9091);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

        // 设置 3s 的 checkpoint 间隔
        env.enableCheckpointing(3000);
        //streamingFileSink 配置
        DataStreamSink<JSONObject> mySQLSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // 设置 source 节点的并行度为 4
                .setParallelism(1)
                //添加处理，到Sink
                .addSink(new JdbcSink()).setParallelism(1)
                .name("Hadoop Sink")
                .uid("hadoop-sink-uid");


        //x秒为一个窗口，执行同步方法
        DataStreamSource<JSONObject> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        mysqlDS.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))).process(new ETLKbossWindowFunction());

        env.execute("MySQL Source");

    }

    /**
     * StreamingFileSink 它可以根据给定的分桶策略将数据写入到不同的 HDFS 文件中
     * 我们传入了 Path 对象来指定数据写入的基本路径，并使用 SimpleStringEncoder 来对数据进行编码。然后，使用 withBucketAssigner 方法设置了时间窗口的分桶策略，这里使用 DateTimeBucketAssigner 来按照时间窗口进行分桶。
     *
     * @param hdfsPath
     * @return
     */
    private static StreamingFileSink<JSONObject> getStreamingFileSink(String hdfsPath) {
        //配置config 前缀，后缀 prefix-x-.txt
        OutputFileConfig config = OutputFileConfig.builder()
                .withPartPrefix("prefix")
                .withPartSuffix(".txt").build();
        // 创建 StreamingFileSink 并设置相关属性

        //OnCheckpointRollingPolicy：在每次Flink检查点完成时滚动到新文件。
        //DefaultRollingPolicy：根据文件大小和时间间隔滚动到新文件。
        //OnFileSizeRollingPolicy：根据文件大小滚动到新文件。
        //OnProcessingTimeRollingPolicy：根据处理时间滚动到新文件。

        return StreamingFileSink
                .forRowFormat(new Path(hdfsPath), new SimpleStringEncoder<JSONObject>("UTF-8"))
                //分桶策略
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd"))
                .withBucketCheckInterval(1000L) // 设置检查新桶的间隔时间，单位：毫秒
                //设置滚动策略 设置零件文件在滚动之前可以保持打开状态的最长时间2min
                //设置允许的不活动间隔，在此间隔之后，零件文件必须滚动
                // 最大容量
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(2))
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                        .withMaxPartSize(1024 * 1024 * 1024).build()
                )
                .withOutputFileConfig(config)
                .build();
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

