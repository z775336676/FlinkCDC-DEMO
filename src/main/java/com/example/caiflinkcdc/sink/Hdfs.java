/**
 * cai-flinkCDC
 * <p>
 * Copyright 2014 Acooly.cn, Inc. All rights reserved.
 *
 * @author 77533
 * @date 2023-07-20 15:20
 */
package com.example.caiflinkcdc.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.*;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author 77533
 * @date 2023-07-20 15:20
 */
@Slf4j
public class Hdfs {
    public static void main(String[] args) throws IOException {
        final String HDFS_PATH = "hdfs://localhost:9000";
        final String HDFS_USER = "root";
        Path path = new Path("/hdfs-api/test/2023-07-20--1505/");

        // 创建 Hadoop 配置对象
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        conf.set("dfs.client.use.datanode.hostname", "true");
        FileSystem fs = null;

        try {
            fs = FileSystem.get(new URI(HDFS_PATH), conf, HDFS_USER);
        } catch (InterruptedException | URISyntaxException e) {
            log.error("", e);
            System.out.println(e);
        }

        // 获取 HDFS 文件夹路径下的所有文件列表
        RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(path, true);

        while (fileIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileIterator.next();
            if (fileStatus.isFile()) {
                Path filePath = fileStatus.getPath();
                // 读取文件数据
                FSDataInputStream inputStream = fs.open(filePath);
                byte[] buffer = new byte[4096]; // 根据实际情况设定缓冲区大小
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) > 0) {
                    // 处理读取的数据
                    System.out.println(new String(buffer, 0, bytesRead, "UTF-8"));
                }
                inputStream.close();
            }
        }

        fs.close();
    }
}

