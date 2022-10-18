package com.atguigu.processfunction;

import com.atguigu.entity.Event;
import com.atguigu.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource()).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                })
        ).process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event event, Context context, Collector<String> collector) throws Exception {
                if (event.user.equals("Mary")) {
                    collector.collect(event.user);
                } else if (event.user.equals("Bob")) {
                    collector.collect(event.user);
                    collector.collect(event.user);
                }
                System.out.println(context.timerService().currentWatermark());
            }
        }).print();

        env.execute();
    }
}
