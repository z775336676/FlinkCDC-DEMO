/**
 * cai-flinkCDC
 * <p>
 * Copyright 2014 Acooly.cn, Inc. All rights reserved.
 *
 * @author 77533
 * @date 2023-07-20 14:25
 */
package com.example.caiflinkcdc.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.fs.Writer;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.StringWriter;

/**
 *
 * @author 77533
 * @date 2023-07-20 14:25
 */
@Slf4j
public class HadoopSink extends StreamingFileSink<String> {
    public HadoopSink(BucketsBuilder<String, ?, ? extends BucketsBuilder<String, ?, ?>> bucketsBuilder, long bucketCheckInterval) {
        super(bucketsBuilder, bucketCheckInterval);
    }


}


