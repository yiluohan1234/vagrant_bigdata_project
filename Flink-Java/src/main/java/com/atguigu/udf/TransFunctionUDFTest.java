package com.atguigu.udf;

import com.atguigu.entity.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransFunctionUDFTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        SingleOutputStreamOperator<Event> stream = clicks.filter(new FlinkFilter("home"));

        stream.print();

        env.execute();
    }

    public static class FlinkFilter implements FilterFunction<Event> {

        private String keyWord;

        public FlinkFilter(String keyWord) {
            this.keyWord = keyWord;
        }

        @Override
        public boolean filter(Event event) throws Exception {
            return event.url.contains(this.keyWord);
        }
    }
}