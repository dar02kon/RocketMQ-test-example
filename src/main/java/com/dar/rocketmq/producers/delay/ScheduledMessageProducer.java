package com.dar.rocketmq.producers.delay;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

/**
 * @author :wx
 * @description : 延迟消息发送
 * @create :2022-09-24 12:23:00
 */
public class ScheduledMessageProducer {
    public static void main(String[] args) throws Exception {
        // 实例化一个生产者以发送预定的消息
        DefaultMQProducer producer = new DefaultMQProducer("ExampleProducerGroup");
        // 启动生产
        producer.start();
        Thread.sleep(3000);
        int totalMessagesToSend = 10;
        for (int i = 0; i < totalMessagesToSend; i++) {
            Message message = new Message("TestTopic", ("Hello scheduled message " + i).getBytes());
            // 此消息将在10秒后发送给消费者。
            message.setDelayTimeLevel(3);
            // 发送消息
            producer.send(message);
        }

        // 使用后关闭生产者。
        producer.shutdown();
    }

}
