package com.atguigu.processfunction;

import com.atguigu.entity.Event;
import com.atguigu.source.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class ProcessingTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 处理时间语义，不需要分配时间戳和 watermark
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 要用定时器，必须基于 KeyedStream
        stream.keyBy(data -> true)
                .process(new KeyedProcessFunction<Boolean, Event, String>() {
                    @Override
                    public void processElement(Event event, Context context, Collector<String> collector) throws Exception {
                        long currTs = context.timerService().currentProcessingTime();
                        collector.collect("数据到达，到达时间：" + new Timestamp(currTs));
                        // 注册一个 10 秒后的定时器
                        context.timerService().registerProcessingTimeTimer(currTs + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        out.collect("定时器触发，触发时间：" + new Timestamp(timestamp));
                    }
                }).print();

        env.execute();
    }
}
