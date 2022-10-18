package com.atguigu.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class sourceFromCustom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 有了自定义的 source function，调用 addSource 方法
        env.addSource(new ClickSource()).setParallelism(2).print();

        env.execute();
    }
}
