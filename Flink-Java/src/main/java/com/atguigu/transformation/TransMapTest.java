package com.atguigu.transformation;

import com.atguigu.entity.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 传入匿名类，实现 MapFunction
        SingleOutputStreamOperator<String> map = stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.url;
            }
        });
//        map.print();

        // 传入 MapFunction 的实现类
        stream.map(new UserExtractor()).print();

        env.execute();
    }

    private static class UserExtractor implements MapFunction<Event, String> {
        @Override
        public String map(Event event) throws Exception {
            return event.url;
        }
    }
}
