package com.atguigu.join;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class CoMapExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3);
        DataStreamSource<Long> stream2 = env.fromElements(1L, 2L, 3L);

        ConnectedStreams<Integer, Long> connectedStream = stream1.connect(stream2);

        SingleOutputStreamOperator<String> result = connectedStream.map(new CoMapFunction<Integer, Long, String>() {
            @Override
            public String map1(Integer integer) throws Exception {
                return "Integer:" + integer;
            }

            @Override
            public String map2(Long aLong) throws Exception {
                return "Long:" + aLong;
            }
        });

        result.print();

        env.execute();
    }
}
