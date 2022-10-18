package com.atguigu.entity;

/**
 * @ClassName OrderEvent
 * @Description 订单事件
 * @Author yiluohan1234
 * @Date 2022/7/15 14:09
 * @Version V1.0
 */
public class OrderEvent {

    public String userId;
    public String orderId;
    public String eventType;
    public Long timestamp;

    public OrderEvent() {
    }

    public OrderEvent(String userId, String orderId, String eventType, Long timestamp) {
        this.userId = userId;
        this.orderId = orderId;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "userId='" + userId + '\'' +
                ", orderId='" + orderId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
