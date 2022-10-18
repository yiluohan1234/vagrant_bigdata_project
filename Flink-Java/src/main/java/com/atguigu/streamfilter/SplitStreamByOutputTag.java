package com.atguigu.streamfilter;

import com.atguigu.entity.Event;
import com.atguigu.source.ClickSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

// 创建环境 -> 读取输入流 -> 生成侧输出流标签 -> process 侧输出流 -> 打印
public class SplitStreamByOutputTag {
    private static OutputTag<Tuple3<String, String, Long>> MaryTag = new OutputTag<Tuple3<String, String, Long>>("Mary-pv") {
        };
    private static OutputTag<Tuple3<String, String, Long>> BobTag = new OutputTag<Tuple3<String, String, Long>>("Bob-pv") {
    };
    private static OutputTag<Tuple3<String, String, Long>> elseTag = new OutputTag<Tuple3<String, String, Long>>("else-pv") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        SingleOutputStreamOperator<Event> processedStream = stream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event event, Context context, Collector<Event> collector) throws Exception {
                if (event.user.equals("Mary")) {
                    context.output(MaryTag, new Tuple3<>(event.user, event.url, event.timestamp));
                } else if (event.user.equals("Bob")) {
                    context.output(BobTag, new Tuple3<>(event.user, event.url, event.timestamp));
                } else {
                    collector.collect(event);
                }
            }
        });

        processedStream.getSideOutput(MaryTag).print("Mary pv");
        processedStream.getSideOutput(BobTag).print("Bob pv");
        processedStream.print("else pv");

        env.execute();
    }

}
