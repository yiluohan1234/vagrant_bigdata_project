package com.atguigu.state;

import com.atguigu.entity.Event;
import com.atguigu.source.ClickSource;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// 创建环境 -> 读取输入流并生成水位线 -> 统计每个用户的点击频次，到达 5 次就输出结果
public class AverageTimestampExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        // 统计每个用户的点击频次，到达 5 次就输出统计结果
        stream.keyBy(data -> data.user)
                .flatMap(new AvgTsResult())
                .print();
        env.execute();
    }

    public static class AvgTsResult extends RichFlatMapFunction<Event, String> {

        // 定义聚合状态，用来计算平均时间戳
        AggregatingState<Event, Long> avgTsAggState;
        // 定义一个值状态，用来保存当前用户访问频次
        ValueState<Long> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            avgTsAggState = getRuntimeContext().getAggregatingState(
                    new AggregatingStateDescriptor<Event, Tuple2<Long, Long>, Long>(
                            "avg-ts", new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                        @Override
                        public Tuple2<Long, Long> createAccumulator() {
                            return Tuple2.of(0L, 0L);
                        }

                        @Override
                        public Tuple2<Long, Long> add(Event event, Tuple2<Long, Long> accumulator) {
                            return Tuple2.of(accumulator.f0 + event.timestamp, accumulator.f1 + 1L);
                        }

                        @Override
                        public Long getResult(Tuple2<Long, Long> accumulator) {
                            return accumulator.f0 / accumulator.f1;
                        }

                        @Override
                        public Tuple2<Long, Long> merge(Tuple2<Long, Long> longLongTuple2, Tuple2<Long, Long> acc1) {
                            return null;
                        }
                    }, Types.TUPLE(Types.LONG, Types.LONG))
            );

            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
        }

        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            Long count = countState.value();
            if (count == null) {
                count = 1L;
            } else {
                count += 1;
            }

            countState.update(count);
            avgTsAggState.add(event);

            // 达到 5 次就输出结果，并清空状态
            if (count == 5){
                collector.collect(event.user + "平均时间戳: " + new TimeStamp(avgTsAggState.get()));
                countState.clear();
            }
        }
    }
}
