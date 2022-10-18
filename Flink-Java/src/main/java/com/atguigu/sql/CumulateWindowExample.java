package com.atguigu.sql;

import com.atguigu.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName CumulateWindowExample
 * @Description test
 * @Author yiluohan1234
 * @Date 2022/7/15
 * @Version V1.0
 * @Logic 读取数据源(生成水位线) -> 创建表环境 -> 将数据流转换为表 -> 注册表 -> 查询
 */
public class CumulateWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源，并分配时间戳、生成水位线
        SingleOutputStreamOperator<Event> eventStream = env.fromElements(
                new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
                new Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
                new Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
                new Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
                new Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                }));

        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 将数据流转换成表，并指定时间属性
        Table eventTable = tableEnv.fromDataStream(
                eventStream,
                $("user"),
                $("url"),
                $("timestamp").rowtime().as("ts") // 将 timestamp 指定为事件时间，并命名为 ts
        );

        // 为方便在 SQL 中引用，在环境中注册表 EventTable
        tableEnv.createTemporaryView("EventTable", eventTable);

        Table result = tableEnv.sqlQuery(
                "SELECT " +
                        "user, " +
                        "window_end AS endT, " +
                        "COUNT(url) AS cnt " +
                   "FROM TABLE( " +
                        "CUMULATE( TABLE EventTable, " +
                        "DESCRIPTOR(ts), " +
                        "INTERVAL '30' MINUTE, " +
                        "INTERVAL '1' HOUR)) " +
                   "GROUP BY user, window_start, window_end "
        );

        tableEnv.toDataStream(result).print();

        env.execute();
    }
}
