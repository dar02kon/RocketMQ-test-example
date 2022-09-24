package com.dar.rocketmq.consumers.pull;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author :wx
 * @description : Lite Pull Consumer Subscribe模式
 * @create :2022-09-24 13:59:00
 */
public class LitePullConsumerSubscribe {
    public static volatile boolean running = true;
    public static void main(String[] args) throws Exception {
        DefaultLitePullConsumer litePullConsumer = new DefaultLitePullConsumer("lite_pull_consumer_test");
        //在subscribe模式下，同一个消费组下的多个LitePullConsumer会负载均衡消费，与PushConsumer一致。
        litePullConsumer.subscribe("TopicTest", "*");
        //setPullBatchSize可以设置每一次拉取的最大消息数量，此外如果不额外设置，LitePullConsumer默认是自动提交位点。
        litePullConsumer.setPullBatchSize(20);
        litePullConsumer.start();
        try {
            while (running) {
                //LitePullConsumer拉取消息调用的是轮询poll接口，如果能拉取到消息则返回对应的消息列表，否则返回null。
                List<MessageExt> messageExts = litePullConsumer.poll();
                System.out.printf("%s%n", messageExts);
            }
        } finally {
            litePullConsumer.shutdown();
        }
    }
}
