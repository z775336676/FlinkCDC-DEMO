package com.example.caiflinkcdc.util;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * 自定义反序列化器
 * @author 77533
 */
public class JsonDeserializationSchema implements DebeziumDeserializationSchema<JSONObject> {

    /*
        期望输出效果
        {
            db:数据库名
            tb:表名
            op:操作类型
            befort:{} 数据修改前，create操作没有该项
            after:{} 数据修改后，delete操作没有该项
        }
         */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<JSONObject> collector) throws Exception {

        JSONObject result = new JSONObject();

        String[] split = sourceRecord.topic().split("\\.");
        result.put("db", split[1]);
        result.put("tb", split[2]);

        //获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        result.put("op", operation.toString().toLowerCase());

        Struct value = (Struct) sourceRecord.value();
        JSONObject after = getValueBeforeAfter(value, "after");
        JSONObject before = getValueBeforeAfter(value, "before");

        if (after != null) {
            result.put("after", after);
        }
        if (before != null) {
            result.put("before", before);
        }

        collector.collect(result);
    }

    public JSONObject getValueBeforeAfter(Struct value, String type) throws ParseException {
        Struct midStr = (Struct) value.get(type);
        JSONObject result = new JSONObject();
        if (midStr != null) {
            List<Field> fields = midStr.schema().fields();
            for (Field field : fields) {
                if (midStr.get(field) != null) {
                    //处理日期格式
                    if ("io.debezium.time.Timestamp".equals(field.schema().name())) {
                        result.put(field.name(), new java.sql.Timestamp(midStr.getInt64(field.name())));
                    } else if ("io.debezium.time.Date".equals(field.schema().name())) {
                        long day = midStr.getInt32(field.name());
                        long millisecond = day * 24 * 60 * 60 * 1000;
                        result.put(field.name(), new java.sql.Date(millisecond));
                    } else if ("io.debezium.time.MicroTime".equals(field.schema().name())) {
                        result.put(field.name(), new java.sql.Time(midStr.getInt64(field.name()) / 1000));
                    } else if ("io.debezium.time.ZonedTimestamp".equals(field.schema().name())) {
                        Date date;
                        String pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'";
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
                        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT+6"));
                        try {
                            date = simpleDateFormat.parse(midStr.getString(field.name()));
                        } catch (ParseException e) {
                            pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
                            simpleDateFormat = new SimpleDateFormat(pattern);
                            simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT+6"));
                            date = simpleDateFormat.parse(midStr.getString(field.name()));
                        }
                        result.put(field.name(), new java.sql.Timestamp(date.getTime()));
                    }else if ("io.debezium.time.MicroTimestamp".equals(field.schema().name())) {
                        result.put(field.name(), new java.sql.Timestamp(midStr.getInt64(field.name())/1000));
                    } else {
                        result.put(field.name(), midStr.get(field));
                    }
                } else {
                    result.put(field.name(), midStr.get(field));
                }
            }
            return result;
        }
        return null;
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return BasicTypeInfo.of(JSONObject.class);
    }

}
