package com.atguigu.sink;

import com.atguigu.entity.Event;
import com.atguigu.source.ClickSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class SinkToRedisTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建一个到 redis 连接的配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("hdp101").build();

        env.addSource(new ClickSource()).addSink(new RedisSink<>(conf, new MyRedisMapper()));

        env.execute();

    }

public static class MyRedisMapper implements RedisMapper<Event> {
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, "clicks");
    }

    @Override
    public String getKeyFromData(Event event) {
        return event.user;
    }

    @Override
    public String getValueFromData(Event event) {
        return event.url;
    }
}
}
