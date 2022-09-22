package com.dar.rocketmq.producers.ordinary;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @author :wx
 * @description : 同步发送
 * @create :2022-09-22 17:03:00
 */
public class SyncProducer {

    public static void main(String[] args) throws MQClientException {
        // 初始化一个producer并设置Producer group name
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name"); //（1）创建一个producer
        // 设置NameServer地址
        producer.setNamesrvAddr("localhost:9876");  //（2）设置 NameServer 的地址(如果有多个NameServer，中间以分号隔开)
        // 启动producer
        producer.start();
        /**
         * producer启动需要一定的时间，如果马上就开启循环发送消息会抛出异常
         */
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < 5; i++) {
            try {
                // 创建一条消息，并指定topic、tag、body等信息，tag可以理解成标签，对消息进行再归类，RocketMQ可以在消费端对tag进行过滤
                Message msg = new Message("TopicTest" /* Topic */,
                        "TagA" /* Tag */,
                        ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                );   //（3）构建消息
                // 利用producer进行发送，并同步等待发送结果
                SendResult sendResult = producer.send(msg);   //（4）调用send接口将消息发送出去
                System.out.printf("%s%n", sendResult);
            }catch (Exception e){
                e.printStackTrace();
            }

        }
        producer.shutdown();

    }

}
