package com.atguigu.watermark;

import com.atguigu.entity.Event;
import com.atguigu.source.ClickSource;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CustomWatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy())
                .print();

        env.execute();
    }

    public static class CustomWatermarkStrategy implements WatermarkStrategy<Event> {

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomPeriodicGenerator();
        }

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event event, long l) {
                    return event.timestamp; // 告诉程序数据源里的时间戳是哪一个字段
                }
            };
        }

        public class CustomPeriodicGenerator implements WatermarkGenerator<Event> {

            private Long delayTime = 5000L; // 延迟时间
            private Long maxTs = Long.MIN_VALUE + delayTime + 1L; // 观察到的最大时间戳

            @Override
            public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
                // 只有在遇到特定的 itemId 时，才发出水位线
                if (event.user.equals("Mary")) {
                    watermarkOutput.emitWatermark(new Watermark(event.timestamp - 1));
                }

            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                // 不需要做任何事情，因为我们在 onEvent 方法中发射了水位线
            }
        }
    }
}
