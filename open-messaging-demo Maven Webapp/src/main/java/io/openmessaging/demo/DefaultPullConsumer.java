package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;


/**
 * @author XF
 *　拉取消费者的默认实现。会实例多个。<p>
 * 主要有两个方法：1、绑定queue和topic 2、获取下一个message对象
 */
public class DefaultPullConsumer implements PullConsumer {
    private MessageStore messageStore = MessageStore.getInstance();
    private KeyValue properties;
    //代表当前消费者消费的queue
    private String queue;
    private Set<String> buckets = new HashSet<>();
    //对应的所有queue和topics
    private List<String> bucketList = new ArrayList<>();

    private int lastIndex = 0;

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
    }


    @Override public KeyValue properties() {
        return properties;
    }

    @Override public Message pull() {

        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Message pull(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Message pull(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Message pull(long timeout, TimeUnit unit, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    /**
     * 轮询拉取下一个消息对象 ，直到把queue与它绑定的topic都消费完(同步方法，锁该对象) <p>
     * 轮询：每次拉取不同queue/topic的下一个message<p>
     * queue是属于对象的属性，需要通过方法attachQueue获得。
     */
    @Override public synchronized Message pullNoWait() {
        if (buckets.size() == 0 || queue == null) {
            return null;
        }
        //use Round Robin 循环
        int checkNum = 0;
        while (++checkNum <= bucketList.size()) {
        	
        	//从下标lastIndex+1开始遍历bucketList，取到一个bucket(轮询，用一次，然后到下一个)
            String bucket = bucketList.get((++lastIndex) % (bucketList.size()));
            
            //用bucket和queue从messageStore取到一个不为null的message
            Message message = messageStore.pullMessage(queue, bucket);
            if (message != null) {
                return message;
            }
        }
        return null;
    }

    @Override public Message pullNoWait(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    /**
     * 为消费者选择queue，并且订阅特定的topics(同步方法，锁该对象)<p>
     * 调用一次本方法，消费者多消费一个queue及对应的topics<p>
     * <p>
     * buckets是set,所以bucketList顺序不固定，但是没有重复。
     */
    @Override public synchronized void attachQueue(String queueName, Collection<String> topics) {
    	//queue不能等于queueName ，相等说明已经附在该queue上了。
        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have alreadly attached to a queue " + queue);
        }
        queue = queueName;
        
        //将queueName与topics都添加到buckets.buckets感觉应该清空，因为拉取时，不同queue的message肯定拉不到。
        buckets.add(queueName);
        buckets.addAll(topics);
        bucketList.clear();
        bucketList.addAll(buckets);
    }

    @Override public void start() {

    }

    @Override public void shutdown() {

    }

}
