package com.atguigu.window;

import com.atguigu.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 将数据源改为 socket 文本流，并转换成 Event 类型
        SingleOutputStreamOperator<Event> stream = env.socketTextStream("hdp101", 8888)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        String[] fields = value.split(",");
                        String user = fields[0].trim();
                        String url = fields[1].trim();
                        String timestamp = fields[2].trim();
                        return new Event(user, url, Long.valueOf(timestamp));
                    }
                });

        // 针对乱序流插入水位线，延迟时间设置为 5s
        stream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        // 抽取时间戳的逻辑
                        return event.timestamp;
                    }
                })
        )
                // 根据 name 分组，开窗统计
                .keyBy(data -> data.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new WatermarkTestReuslt())
                .print();

        env.execute();
    }

    // 自定义处理窗口函数，输出当前的水位线和窗口信息
    public static class WatermarkTestReuslt extends ProcessWindowFunction<Event, String, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Event> iterable, Collector<String> collector) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            long currentWatermark = context.currentWatermark();
            long count = iterable.spliterator().getExactSizeIfKnown();
            collector.collect("窗口" + start + "~" + end + "中共有" + count +
                    "个元素,窗口闭合计算时，水位线位于：" + currentWatermark );

        }
    }
}
