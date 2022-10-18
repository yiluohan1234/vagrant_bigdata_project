package com.atguigu.streamfilter;

import com.atguigu.entity.Event;
import com.atguigu.source.ClickSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 创建环境 -> 读取输入流 -> 筛选 Mary，Bob else 生成流 -> 打印
public class SplitStreamByFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 筛选Mary的浏览行为放入 maryStream 流中
        SingleOutputStreamOperator<Event> maryStream = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.user.equals("Mary");
            }
        });

        // 筛选Bob的浏览行为放入 bobStream 流中
        SingleOutputStreamOperator<Event> bobStream = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.user.equals("Bob");
            }
        });

        // 筛选其他人的浏览行为放入 elseStream 流中
        SingleOutputStreamOperator<Event> elseStream = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return !event.user.equals("Mary") && !event.user.equals("Bob");
            }
        });

        maryStream.print("Mary pv");
        bobStream.print("Bob pv");
        elseStream.print("else pv");

        env.execute();

    }
}
