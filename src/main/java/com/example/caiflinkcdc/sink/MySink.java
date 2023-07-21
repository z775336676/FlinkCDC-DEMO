package com.example.caiflinkcdc.sink;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author 77533
 */
public class MySink implements SinkFunction<String> {
    @Override
    public void invoke(String value, Context context) throws Exception {
        /**
         *         c：表示插入（create）操作。
         *        u：表示更新（update）操作。
         *        d：表示删除（delete）操作。
         */
        final String HDFS_PATH = "hdfs://localhost:9000";
        final String HDFS_USER = "root";
        Path path = new Path("/hdfs-api/test/member.txt");

        JSONObject jsonObjectBody = JSONObject.parseObject(value);
        //操作类型
        String op = jsonObjectBody.getString("op");
        JSONObject after = jsonObjectBody.getJSONObject("after");
        JSONObject before = jsonObjectBody.getJSONObject("before");
        JSONObject source = jsonObjectBody.getJSONObject("source");
        //表名
        String table = source.getString("table");
        //数据库名
        String dataBase = source.getString("db");

        System.out.println("=====OP:" + op);
        System.out.println("=====db:" + dataBase);
        System.out.println("=====tb:" + table);
        System.out.println("=====after:" + after);
        System.out.println("=====before:" + before);
        System.out.println("=====source:" + source);
        // 在这里进行数据处理
        System.out.println("===========================Received data: " + value);

        // 进行进一步的业务处理逻辑

        // 存入 创建 Hadoop 配置对象
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        conf.set("dfs.client.use.datanode.hostname", "true");
        FileSystem fs = FileSystem.get(new URI(HDFS_PATH), conf, HDFS_USER);
        // 如果文件存在，默认会覆盖,若为false 则会报错 可以通过第二个参数进行控制。第三个参数可以控制使用缓冲区的大小
        FSDataOutputStream out = fs.create(path,
                true, 4096);
        out.write(value.getBytes());
        // 强制将缓冲区中内容刷出
        out.flush();
        out.close();
        FSDataInputStream inputStream = fs.open(path);
        String data = inputStreamToString(inputStream, "utf-8");
        System.out.println("===================数据:" + data);
        // 关闭文件系统
        fs.close();
    }

    private static String inputStreamToString(InputStream inputStream, String encode) {
        try {
            if (encode == null || ("".equals(encode))) {
                encode = "utf-8";
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, encode));
            StringBuilder builder = new StringBuilder();
            String str = "";
            while ((str = reader.readLine()) != null) {
                builder.append(str).append("\n");
            }
            return builder.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


}



