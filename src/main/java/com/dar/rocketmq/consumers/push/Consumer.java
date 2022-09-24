package com.dar.rocketmq.consumers.push;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author :wx
 * @description : Push消费
 * @create :2022-09-24 13:38:00
 */
public class Consumer {
    public static void main(String[] args) throws InterruptedException, MQClientException {
        // 初始化consumer，并设置consumer group name
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name");

        // 设置NameServer地址
        consumer.setNamesrvAddr("localhost:9876");
        //订阅一个或多个topic，并指定tag过滤条件，这里指定*表示接收所有tag的消息
        consumer.subscribe("TopicTest", "*");
        //注册回调接口来处理从Broker中收到的消息
        //并发消费
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                // 返回消息消费状态，ConsumeConcurrentlyStatus.CONSUME_SUCCESS为消费成功
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });


        /**
         *          //顺序消费
         *          consumer.registerMessageListener(new MessageListenerOrderly() {
         *             AtomicLong consumeTimes = new AtomicLong(0);
         *             @Override
         *             public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
         *                 System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
         *                 this.consumeTimes.incrementAndGet();
         *                 if ((this.consumeTimes.get() % 2) == 0) {
         *                     return ConsumeOrderlyStatus.SUCCESS;
         *                 } else if ((this.consumeTimes.get() % 5) == 0) {
         *                     context.setSuspendCurrentQueueTimeMillis(3000);
         *                     return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
         *                 }
         *                 return ConsumeOrderlyStatus.SUCCESS;
         *             }
         *         });
         */


        // 启动Consumer
        consumer.start();
        System.out.printf("Consumer Started.%n");
        Thread.sleep(5000);
        //不手动关闭consumer，进程会一直持续下去
        consumer.shutdown();
    }
}
