package com.dar.rocketmq.consumers.pull;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * @author :wx
 * @description : Pull Consumer示例
 * @create :2022-09-24 13:52:00
 */
public class PullConsumerTest {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("please_rename_unique_group_name_5");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.start();
        Thread.sleep(3000);
        try {
            MessageQueue mq = new MessageQueue();
            mq.setQueueId(0);
            mq.setTopic("TopicTest");
            mq.setBrokerName("LAPTOP-UD2UCLT6");//改成自己的BrokerName
            long offset = 26;
            PullResult pullResult = consumer.pull(mq, "*", offset, 32);
            if (pullResult.getPullStatus().equals(PullStatus.FOUND)) {
                System.out.printf("%s%n", pullResult.getMsgFoundList());
                consumer.updateConsumeOffset(mq, pullResult.getNextBeginOffset());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        consumer.shutdown();
    }
}
