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

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        final String HDFS_PATH = "hdfs://localhost:9000";
        final String HDFS_USER = "root";
        Path path = new Path("/hdfs-api/test/a.txt");
        try {
            // 创建 Hadoop 配置对象
            Configuration conf = new Configuration();
            conf.set("dfs.replication", "1");
            conf.set("dfs.client.use.datanode.hostname", "true");
            FileSystem fs = FileSystem.get(new URI(HDFS_PATH), conf, HDFS_USER);

            // 检查文件是否存在，如果存在则删除
//            System.out.println(fs.exists(path));
//            if (fs.exists(path)) {
//                fs.delete(path, true);
//            }

            //创建指定权限的目录
//            fs.mkdirs(new Path("/hdfs-api/test1/"),
//                    new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.READ));

            // 如果文件存在，默认会覆盖,若为false 则会报错 可以通过第二个参数进行控制。第三个参数可以控制使用缓冲区的大小
//            createInsert(path, fs);

            //新增数据
//            insertData(fs.append(path), "新增数据");

            //读取文件
            getFile(fs,path);

            System.out.println("Data written to HDFS successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建文件 写入数据
     * @param path
     * @param fs
     * @throws IOException
     */
    private static void createInsert(Path path, FileSystem fs) throws IOException {
        FSDataOutputStream out = fs.create(path,
                true, 4096);
        out.write("hello hadoop!".getBytes());
        out.write("hello spark!".getBytes());
        out.write("hello flink!".getBytes());
        // 强制将缓冲区中内容刷出
        out.flush();
        out.close();
    }

    /**
     * 写入新数据
     * @param fs
     * @param s
     * @throws IOException
     */
    private static void insertData(FSDataOutputStream fs, String s) throws IOException {
        fs.write(s.getBytes());
        fs.flush();
        fs.close();
    }

    /**
     * 读取文件
     */
    private static void getFile(FileSystem fs, Path path) throws IOException {
        FSDataInputStream inputStream = fs.open(path);
        String data = inputStreamToString(inputStream, "utf-8");
        System.out.println("===================数据:"+data);
        byte[] buffer = new byte[4096]; // 根据实际情况设定缓冲区大小
        int bytesRead;
        while ((bytesRead = inputStream.read(buffer)) > 0) {
            // 处理读取的数据
            System.out.println("===================数据:" + new String(buffer, 0, bytesRead, "UTF-8"));
        }
        inputStream.close();
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



