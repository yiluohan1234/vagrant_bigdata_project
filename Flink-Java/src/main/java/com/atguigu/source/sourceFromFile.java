package com.atguigu.source;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class sourceFromFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());

//        DataStreamSource<String> stream = env.readTextFile("datas/flink/words.txt");
        DataStreamSource<String> stream = env.readTextFile("hdfs://hdp101:8020/hbase/hbase.id");
        stream.print();

        env.execute();
    }
}
