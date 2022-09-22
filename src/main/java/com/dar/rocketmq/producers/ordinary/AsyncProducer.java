package com.dar.rocketmq.producers.ordinary;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @author :wx
 * @description : 异步发送
 * @create :2022-09-22 17:40:00
 */
public class AsyncProducer {
    public static void main(String[] args) throws Exception {
        // 初始化一个producer并设置Producer group name
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
        // 设置NameServer地址
        producer.setNamesrvAddr("localhost:9876");
        // 启动producer
        producer.start();
        producer.setRetryTimesWhenSendAsyncFailed(0);
        Thread.sleep(3000);//等producer启动完成
        for (int i = 0; i < 1; i++) {
            try {
                final int index = i;
                // 创建一条消息，并指定topic、tag、body等信息，tag可以理解成标签，对消息进行再归类，RocketMQ可以在消费端对tag进行过滤
                Message msg = new Message("TopicTest",
                        "TagA",
                        "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
                // 异步发送消息, 发送结果通过callback返回给客户端
                producer.send(msg, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        System.out.printf("%-10d OK %s %n", index,
                                sendResult.getMsgId());
                    }

                    @Override
                    public void onException(Throwable e) {
                        System.out.printf("%-10d Exception %s %n", index, e);
                        e.printStackTrace();
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        /**
         * 由于是异步发送需要在关闭producer前适当休眠，
         * 避免在消息发送前，producer已经关闭导致报错
         */
        Thread.sleep(3000);
        // 一旦producer不再使用，关闭producer
        producer.shutdown();
    }
}
