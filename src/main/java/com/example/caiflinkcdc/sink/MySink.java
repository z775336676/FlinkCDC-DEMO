package com.example.caiflinkcdc.sink;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author 77533
 */
public class MySink implements SinkFunction<String> {
    @Override
    public void invoke(String value, Context context) {
        /**
         *         c：表示插入（create）操作。
         *        u：表示更新（update）操作。
         *        d：表示删除（delete）操作。
         */
//
//        //操作类型
//        String op = value.getString("op");
//        //数据库名
//        String db = value.getString("db");
//        //表名
//        String tb = value.getString("tb");
//        JSONObject after = value.getJSONObject("after");
//        JSONObject before = value.getJSONObject("before");

        // 在这里进行数据处理
        System.out.println("===========================Received data: " + value);
        // 进行进一步的业务处理逻辑
    }
}
