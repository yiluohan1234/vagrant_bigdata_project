package com.atguigu.cep;

import com.atguigu.entity.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @ClassName OrderTimeoutDetect
 * @Description 订单超时未支付，取消订单
 * @Author yiluohan1234
 * @Date 2022/7/15
 * @Version V1.0
 * @Logic 获取订单事件流（生成水位线） -> 定义 Pattern -> 将 Pattern 作用到流上 -> 匹配到的和超时时间打印出来
 */
public class OrderTimeoutDetect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取订单事件流，并提取时间戳、生成水位线
        KeyedStream<OrderEvent, String> stream = env.fromElements(
                new OrderEvent("user_1", "order_1", "create", 1000L),
                new OrderEvent("user_2", "order_2", "create", 2000L),
                new OrderEvent("user_1", "order_1", "modify", 10 * 1000L),
                new OrderEvent("user_1", "order_1", "pay", 60 * 1000L),
                new OrderEvent("user_2", "order_3", "create", 10 * 60 * 1000L),
                new OrderEvent("user_2", "order_3", "pay", 20 * 60 * 1000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent orderEvent, long l) {
                        return orderEvent.timestamp;
                    }
                })).keyBy(r -> r.orderId);

        // 1. 定义 Pattern
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("create") // 首先是下单事件
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return orderEvent.eventType.equals("create");
                    }
                })
                .followedBy("pay") // 之后是支付事件；中间可以修改订单，宽松近邻
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return orderEvent.eventType.equals("pay");
                    }
                }).within(Time.minutes(15)); // 限制在 15 分钟之内

        // 2. 将 Pattern 应用到流上，检测匹配的复杂事件，得到一个 PatternStream
        PatternStream<OrderEvent> patterStream = CEP.pattern(stream, pattern);

        // 3. 将匹配到的，和超时部分匹配的复杂事件提取出来，然后包装成提示信息输出
        SingleOutputStreamOperator<String> payedOrderStream = patterStream.process(new OrderPayPatternProcessFunction());

        // 将正常匹配和超时部分匹配的处理结果流打印输出
        payedOrderStream.print("payed");
        OutputTag<String> timeout = new OutputTag<String>("timeout") {
        };
        payedOrderStream.getSideOutput(timeout).print("timeout");

        env.execute();


    }

    // 实现自定义的 PatternProcessFunction，需实现 TimedOutPartialMatchHandler 接口
    public static class OrderPayPatternProcessFunction extends PatternProcessFunction<OrderEvent, String> implements TimedOutPartialMatchHandler<OrderEvent> {
        // 处理正常匹配事件
        @Override
        public void processMatch(Map<String, List<OrderEvent>> map, Context context, Collector<String> collector) throws Exception {
            OrderEvent payEvent = map.get("pay").get(0);
            collector.collect("订单 " + payEvent.orderId + " 已支付！");
        }

        // 处理超时未支付事件
        @Override
        public void processTimedOutMatch(Map<String, List<OrderEvent>> map, Context context) throws Exception {
            OrderEvent createEvent = map.get("create").get(0);
            context.output(new OutputTag<String>("timeout"){}, "订单 " + createEvent.orderId + " 超时未支付！用户为：" + createEvent.userId);
        }
    }
}
