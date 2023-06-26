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

        JSONObject jsonObjectBody = JSONObject.parseObject(value);
        //操作类型
        String op = jsonObjectBody.getString("op");
        JSONObject after = jsonObjectBody.getJSONObject("after");
        JSONObject before = jsonObjectBody.getJSONObject("before");
        JSONObject source = jsonObjectBody.getJSONObject("source");
        //表名
        JSONObject table = source.getJSONObject("table");
        //数据库名
        JSONObject dataBase = source.getJSONObject("db");

        System.out.println("=====OP:"+op);
        System.out.println("=====db:"+dataBase);
        System.out.println("=====tb:"+table);
        System.out.println("=====after:"+after);
        System.out.println("=====before:"+before);
        System.out.println("=====source:"+source);
        // 在这里进行数据处理
        System.out.println("===========================Received data: " + value);
        // 进行进一步的业务处理逻辑
    }
}
