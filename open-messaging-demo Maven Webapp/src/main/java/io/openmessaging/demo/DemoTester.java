package io.openmessaging.demo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;
import io.openmessaging.PullConsumer;
import junit.framework.Assert;

public class DemoTester {


    public static void main(String[] args) {
        KeyValue properties = new DefaultKeyValue();
        properties.put("STORE_PATH", "/home/admin/test"); //瀹為檯娴嬭瘯鏃跺埄鐢� STORE_PATH 浼犲叆瀛樺偍璺緞

        //杩欎釜娴嬭瘯绋嬪簭鐨勬祴璇曢�昏緫涓庡疄闄呰瘎娴嬬浉浼硷紝浣嗘敞鎰忚繖閲屾槸鍗曠嚎绋嬬殑锛屽疄闄呮祴璇曟椂浼氭槸澶氱嚎绋嬬殑锛屽苟涓斿彂閫佸畬涔嬪悗浼欿ill杩涚▼锛屽啀璧锋秷璐归�昏緫

        Producer producer = new DefaultProducer(properties);

        //鏋勯�犳祴璇曟暟鎹�
        String topic1 = "TOPIC1"; //瀹為檯娴嬭瘯鏃跺ぇ姒備細鏈�100涓猅opic宸﹀彸
        String topic2 = "TOPIC2"; //瀹為檯娴嬭瘯鏃跺ぇ姒備細鏈�100涓猅opic宸﹀彸
        String queue1 = "QUEUE1"; //瀹為檯娴嬭瘯鏃跺ぇ姒備細鏈�100涓猀ueue宸﹀彸
        String queue2 = "QUEUE2"; //瀹為檯娴嬭瘯鏃跺ぇ姒備細鏈�100涓猀ueue宸﹀彸
        List<Message> messagesForTopic1 = new ArrayList<>(1024);
        List<Message> messagesForTopic2 = new ArrayList<>(1024);
        List<Message> messagesForQueue1 = new ArrayList<>(1024);
        List<Message> messagesForQueue2 = new ArrayList<>(1024);
        for (int i = 0; i < 1024; i++) {
            messagesForTopic1.add(producer.createBytesMessageToTopic(topic1,  (topic1 + i).getBytes()));
            messagesForTopic2.add(producer.createBytesMessageToTopic(topic2,  (topic2 + i).getBytes()));
            messagesForQueue1.add(producer.createBytesMessageToQueue(queue1, (queue1 + i).getBytes()));
            messagesForQueue2.add(producer.createBytesMessageToQueue(queue2, (queue2 + i).getBytes()));
        }

        long start = System.currentTimeMillis();
        //鍙戦��, 瀹為檯娴嬭瘯鏃讹紝浼氱敤澶氱嚎绋嬫潵鍙戦��, 姣忎釜绾跨▼鍙戦�佽嚜宸辩殑Topic鍜孮ueue
        for (int i = 0; i < 1024; i++) {
            producer.send(messagesForTopic1.get(i));
            producer.send(messagesForTopic2.get(i));
            producer.send(messagesForQueue1.get(i));
            producer.send(messagesForQueue2.get(i));
        }
        long end = System.currentTimeMillis();

        long T1 = end - start;

        //娑堣垂鏍蜂緥1锛屽疄闄呮祴璇曟椂浼欿ill鎺夊彂閫佽繘绋嬶紝鍙﹀彇杩涚▼杩涜娑堣垂
        {
            PullConsumer consumer1 = new DefaultPullConsumer(properties);
            consumer1.attachQueue(queue1, Collections.singletonList(topic1));

            int queue1Offset = 0, topic1Offset = 0;

            long startConsumer = System.currentTimeMillis();
            while (true) {
                Message message = consumer1.pullNoWait();
                if (message == null) {
                    //鎷夊彇涓簄ull鍒欒涓烘秷鎭凡缁忔媺鍙栧畬姣�
                    break;
                }
                String topic = message.headers().getString(MessageHeader.TOPIC);
                String queue = message.headers().getString(MessageHeader.QUEUE);
                //瀹為檯娴嬭瘯鏃讹紝浼氫竴涓�姣旇緝鍚勪釜瀛楁
                if (topic != null) {
                    Assert.assertEquals(topic1, topic);
                    Assert.assertEquals(messagesForTopic1.get(topic1Offset++), message);
                } else {
                    Assert.assertEquals(queue1, queue);
                    Assert.assertEquals(messagesForQueue1.get(queue1Offset++), message);
                }
            }
            long endConsumer = System.currentTimeMillis();
            long T2 = endConsumer - startConsumer;
            System.out.println(String.format("Team1 cost:%d ms tps:%d q/ms", T2 + T1, (queue1Offset + topic1Offset)/(T1 + T2)));

        }

        //娑堣垂鏍蜂緥2锛屽疄闄呮祴璇曟椂浼欿ill鎺夊彂閫佽繘绋嬶紝鍙﹀彇杩涚▼杩涜娑堣垂
        {
            PullConsumer consumer2 = new DefaultPullConsumer(properties);
            List<String> topics = new ArrayList<>();
            topics.add(topic1);
            topics.add(topic2);
            consumer2.attachQueue(queue2, topics);

            int queue2Offset = 0, topic1Offset = 0, topic2Offset = 0;

            long startConsumer = System.currentTimeMillis();
            while (true) {
                Message message = consumer2.pullNoWait();
                if (message == null) {
                    //鎷夊彇涓簄ull鍒欒涓烘秷鎭凡缁忔媺鍙栧畬姣�
                    break;
                }

                String topic = message.headers().getString(MessageHeader.TOPIC);
                String queue = message.headers().getString(MessageHeader.QUEUE);
                //瀹為檯娴嬭瘯鏃讹紝浼氫竴涓�姣旇緝鍚勪釜瀛楁
                if (topic != null) {
                    if (topic.equals(topic1)) {
                        Assert.assertEquals(messagesForTopic1.get(topic1Offset++), message);
                    } else {
                        Assert.assertEquals(topic2, topic);
                        Assert.assertEquals(messagesForTopic2.get(topic2Offset++), message);
                    }
                } else {
                    Assert.assertEquals(queue2, queue);
                    Assert.assertEquals(messagesForQueue2.get(queue2Offset++), message);
                }
            }
            long endConsumer = System.currentTimeMillis();
            long T2 = endConsumer - startConsumer;
            System.out.println(String.format("Team2 cost:%d ms tps:%d q/ms", T2 + T1, (queue2Offset + topic1Offset)/(T1 + T2)));
        }


    }
}
