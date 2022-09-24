package com.dar.rocketmq.producers.batch;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * @author :wx
 * @description : 批量消息发送
 * @create :2022-09-24 12:32:00
 */
public class SimpleBatchProducer {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("BatchProducerGroupName");
        producer.start();

        Thread.sleep(3000);
        //如果每次只发送不超过1MiB的消息，则很容易使用批处理
        //同一批的消息应该有:相同的主题、相同的waitStoreMsgOK和不支持调度
        String topic = "BatchTest";
        List<Message> messages = new ArrayList<>();
        messages.add(new Message(topic, "Tag", "OrderID001", "Hello world 0".getBytes()));
        messages.add(new Message(topic, "Tag", "OrderID002", "Hello world 1".getBytes()));
        messages.add(new Message(topic, "Tag", "OrderID003", "Hello world 2".getBytes()));
        producer.send(messages);

        producer.shutdown();
    }
}
