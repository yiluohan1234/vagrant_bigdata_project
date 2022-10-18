package com.atguigu.source;

import com.atguigu.entity.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class sourceFromCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        ArrayList<Event> clicks = new ArrayList<>();
        clicks.add(new Event("zhangsan", "./home", 1000L));
        clicks.add(new Event("lisi", "./cart", 2000L));

        DataStreamSource<Event> stream = env.fromCollection(clicks);


        stream.print();

        env.execute();

    }
}
