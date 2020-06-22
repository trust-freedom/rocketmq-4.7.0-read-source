/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {
    private static final InternalLogger log = ClientLogger.getLog();

    // 消息推模式实现类，MQConsumerInner实现类
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

    // 用户使用的消费者对象，主要用于获取参数
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final MessageListenerConcurrently messageListener;

    // consumeExecutor线程池使用的，承载消费任务的阻塞队列
    private final BlockingQueue<Runnable> consumeRequestQueue;

    // 消息消费线程池
    private final ThreadPoolExecutor consumeExecutor;

    private final String consumerGroup;

    // 添加消费任务到consumeExecutor的延迟调度器
    private final ScheduledExecutorService scheduledExecutorService;

    // 定时删除过期消息线程池
    private final ScheduledExecutorService cleanExpireMsgExecutors;


    public ConsumeMessageConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
        MessageListenerConcurrently messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        this.consumeExecutor = new ThreadPoolExecutor(
            this.defaultMQPushConsumer.getConsumeThreadMin(),     // 消费线程池最小的线程数，默认20
            this.defaultMQPushConsumer.getConsumeThreadMax(), // 消费线程池最大的线程数，默认20
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.consumeRequestQueue,
            new ThreadFactoryImpl("ConsumeMessageThread_"));

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
        this.cleanExpireMsgExecutors = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("CleanExpireMsgScheduledThread_"));
    }

    public void start() {
        this.cleanExpireMsgExecutors.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                cleanExpireMsg();
            }

        }, this.defaultMQPushConsumer.getConsumeTimeout(), this.defaultMQPushConsumer.getConsumeTimeout(), TimeUnit.MINUTES);
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
        this.consumeExecutor.shutdown();
        this.cleanExpireMsgExecutors.shutdown();
    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0
            && corePoolSize <= Short.MAX_VALUE
            && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    @Override
    public void incCorePoolSize() {
        // long corePoolSize = this.consumeExecutor.getCorePoolSize();
        // if (corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax())
        // {
        // this.consumeExecutor.setCorePoolSize(this.consumeExecutor.getCorePoolSize()
        // + 1);
        // }
        // log.info("incCorePoolSize Concurrently from {} to {}, ConsumerGroup:
        // {}",
        // corePoolSize,
        // this.consumeExecutor.getCorePoolSize(),
        // this.consumerGroup);
    }

    @Override
    public void decCorePoolSize() {
        // long corePoolSize = this.consumeExecutor.getCorePoolSize();
        // if (corePoolSize > this.defaultMQPushConsumer.getConsumeThreadMin())
        // {
        // this.consumeExecutor.setCorePoolSize(this.consumeExecutor.getCorePoolSize()
        // - 1);
        // }
        // log.info("decCorePoolSize Concurrently from {} to {}, ConsumerGroup:
        // {}",
        // corePoolSize,
        // this.consumeExecutor.getCorePoolSize(),
        // this.consumerGroup);
    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }


    /**
     * 直接消费消息
     * @param msg
     * @param brokerName
     * @return
     */
    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(false);
        result.setAutoCommit(true);

        List<MessageExt> msgs = new ArrayList<MessageExt>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(mq);

        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);

        try {
            ConsumeConcurrentlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                switch (status) {
                    case CONSUME_SUCCESS:
                        result.setConsumeResult(CMResult.CR_SUCCESS);
                        break;
                    case RECONSUME_LATER:
                        result.setConsumeResult(CMResult.CR_LATER);
                        break;
                    default:
                        break;
                }
            } else {
                result.setConsumeResult(CMResult.CR_RETURN_NULL);
            }
        } catch (Throwable e) {
            result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
            result.setRemark(RemotingHelper.exceptionSimpleDesc(e));

            log.warn(String.format("consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s",
                RemotingHelper.exceptionSimpleDesc(e),
                ConsumeMessageConcurrentlyService.this.consumerGroup,
                msgs,
                mq), e);
        }

        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }


    /**
     * 向消费线程池中提交请求，每个ConsumeRequest包含最多ConsumeMessageBatchMaxSize条消息
     * @param msgs                消息列表，默认一次最多从Broker拉取32条消息
     * @param processQueue        消息处理队列
     * @param messageQueue        消息所属消费队列
     * @param dispatchToConsume   是否转发到消费线程池，顺序消费时会用到
     */
    @Override
    public void submitConsumeRequest(
        final List<MessageExt> msgs,
        final ProcessQueue processQueue,
        final MessageQueue messageQueue,
        final boolean dispatchToConsume) {
        final int consumeBatchSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize(); // 默认为 1

        // 拉取到的消息 小于 consumeMessageBatchMaxSize，只构造一个ConsumeRequest，向消费线程池提交一个任务
        if (msgs.size() <= consumeBatchSize) {
            ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
            try {
                this.consumeExecutor.submit(consumeRequest);
            } catch (RejectedExecutionException e) {
                // 被线程池拒绝，延迟5秒再提交
                // 但由于consumeExecutor使用的是LinkedBlockingQueue无界队列，实际这个方法调用不到
                this.submitConsumeRequestLater(consumeRequest);
            }
        }
        // 拉取到的消息 大于 consumeMessageBatchMaxSize，每consumeMessageBatchMaxSize条消息向消费线程池提交一个任务
        else {
            for (int total = 0; total < msgs.size(); ) {
                List<MessageExt> msgThis = new ArrayList<MessageExt>(consumeBatchSize);
                for (int i = 0; i < consumeBatchSize; i++, total++) {
                    if (total < msgs.size()) {
                        msgThis.add(msgs.get(total));
                    } else {
                        break;
                    }
                }

                ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, processQueue, messageQueue);
                try {
                    this.consumeExecutor.submit(consumeRequest);
                } catch (RejectedExecutionException e) {
                    // 将消息放回去
                    for (; total < msgs.size(); total++) {
                        msgThis.add(msgs.get(total));
                    }

                    // 被线程池拒绝，延迟5秒再提交
                    // 但由于consumeExecutor使用的是LinkedBlockingQueue无界队列，实际这个方法调用不到
                    this.submitConsumeRequestLater(consumeRequest);
                }
            }
        }
    }


    private void cleanExpireMsg() {
        Iterator<Map.Entry<MessageQueue, ProcessQueue>> it =
            this.defaultMQPushConsumerImpl.getRebalanceImpl().getProcessQueueTable().entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, ProcessQueue> next = it.next();
            ProcessQueue pq = next.getValue();
            pq.cleanExpiredMsg(this.defaultMQPushConsumer);
        }
    }


    /**
     * 处理MessageListener的消费结果
     * @param status
     * @param context
     * @param consumeRequest
     */
    public void processConsumeResult(
        final ConsumeConcurrentlyStatus status,
        final ConsumeConcurrentlyContext context,
        final ConsumeRequest consumeRequest
    ) {
        int ackIndex = context.getAckIndex();

        if (consumeRequest.getMsgs().isEmpty())
            return;

        /**
         * 根据status，维护内存中的消费状态信息
         */
        switch (status) {
            case CONSUME_SUCCESS:
                if (ackIndex >= consumeRequest.getMsgs().size()) {
                    ackIndex = consumeRequest.getMsgs().size() - 1;
                }
                int ok = ackIndex + 1;
                int failed = consumeRequest.getMsgs().size() - ok;
                this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), ok);
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), failed);
                break;
            case RECONSUME_LATER:
                ackIndex = -1; // 在集群消费下，会把consumeRequest.getMsgs()的消息从下标0都发回broker
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(),
                    consumeRequest.getMsgs().size());
                break;
            default:
                break;
        }


        /**
         * 如果消费失败，需要重新发回broker
         * 根据不同消费模式执行逻辑
         */
        switch (this.defaultMQPushConsumer.getMessageModel()) {
            /**
             * 广播
             * 业务方返回 RECONSUME_LATER ，消息并不会重新被消费，只是以警告级别输出到日志文件
             */
            case BROADCASTING:
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    log.warn("BROADCASTING, the message consume failed, drop it, {}", msg.toString());
                }
                break;
            /**
             * 集群
             * 在业务方返回 RECONSUME_LATER 时，该批消息都需要发 ACK 消息
             * 如果消息发送 ACK 失败，则直接将本批 ACK 消费发送失败的消息再次封装为 ConsumeRequest ，然后延迟 Ss 后重新消费
             * 如果 ACK 消息发送成功，则该消息会延迟消费
             */
            case CLUSTERING:
                List<MessageExt> msgBackFailed = new ArrayList<MessageExt>(consumeRequest.getMsgs().size());
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    // 逐条发回broker
                    boolean result = this.sendMessageBack(msg, context);
                    if (!result) {
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                        msgBackFailed.add(msg);
                    }
                }

                if (!msgBackFailed.isEmpty()) {
                    consumeRequest.getMsgs().removeAll(msgBackFailed);

                    this.submitConsumeRequestLater(msgBackFailed, consumeRequest.getProcessQueue(), consumeRequest.getMessageQueue());
                }
                break;
            default:
                break;
        }

        /**
         * 更新offset偏移量
         * ProcessQueue.removeMessage()
         * 返回的偏移量是移除该批消息后ProcessQueue中的msgTreeMap.firstKey最小的偏移量，然后用该偏移量更新消息消费进度 ，以便在消费者重启后能从上一次的消费进度开始消费，避免消息重复消费
         * 自己：但如果并发消费了10条消息，偏移量从1-10，当前线程消费的是第5条，removeMessage后返回的偏移量是1？？
         *
         * 值得重点注意的是当消息监听器返回 RECONSUME_LATER ，消息消费进度也会向前推进
         * 用ProcessQueue中最小的队列偏移量调用消息消费进度存储器 OffsetStore 更新消费进度
         * 这是因为当返回 RECONSUME_LATER, RocketMQ会创建一条与原先消息属性相同的消息，拥有一个唯一的新msgld ，并存储原消息ID
         * 该消息会存入到commitlog文件中，与原先的消息没有任何关联，那该消息当然也会进入到ConsuemeQueue队列中，将拥有一个全新的队列偏移量
         */
        long offset = consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs());
        if (offset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), offset, true);
        }
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    public boolean sendMessageBack(final MessageExt msg, final ConsumeConcurrentlyContext context) {
        int delayLevel = context.getDelayLevelWhenNextConsume();

        // Wrap topic with namespace before sending back message.
        msg.setTopic(this.defaultMQPushConsumer.withNamespace(msg.getTopic()));
        try {
            this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, context.getMessageQueue().getBrokerName());
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(), e);
        }

        return false;
    }

    private void submitConsumeRequestLater(
        final List<MessageExt> msgs,
        final ProcessQueue processQueue,
        final MessageQueue messageQueue
    ) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.submitConsumeRequest(msgs, processQueue, messageQueue, true);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }


    /**
     * 延迟5秒再提交到consumeExecutor线程池
     * 但由于consumeExecutor使用的是LinkedBlockingQueue无界队列，实际这个方法调用不到
     * @param consumeRequest
     */
    private void submitConsumeRequestLater(final ConsumeRequest consumeRequest) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.consumeExecutor.submit(consumeRequest);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }


    /**
     * 消费消息的线程任务（并发）
     * 可以看到msgs是直接传进来的，不是从processQueue获取的
     */
    class ConsumeRequest implements Runnable {
        private final List<MessageExt> msgs;
        private final ProcessQueue processQueue;
        private final MessageQueue messageQueue;

        public ConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue) {
            this.msgs = msgs;
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }

        public List<MessageExt> getMsgs() {
            return msgs;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        @Override
        public void run() {
            // processQueue 如果被 drooped，直接退出
            // 在进行消息重新负载时如果该消息队列被分配给消费组内其他消费者后，需要 droped 设置为 true，阻止消费者继续消费不属于自己的消息队列
            if (this.processQueue.isDropped()) {
                log.info("the message queue not be able to consume, because it's dropped. group={} {}", ConsumeMessageConcurrentlyService.this.consumerGroup, this.messageQueue);
                return;
            }

            MessageListenerConcurrently listener = ConsumeMessageConcurrentlyService.this.messageListener;
            ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
            ConsumeConcurrentlyStatus status = null;
            // 对Retry重试队列信息进行重置
            defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());

            ConsumeMessageContext consumeMessageContext = null;

            // 如果有，执行消费前Hook
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext = new ConsumeMessageContext();
                consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                consumeMessageContext.setConsumerGroup(defaultMQPushConsumer.getConsumerGroup());
                consumeMessageContext.setProps(new HashMap<String, String>());
                consumeMessageContext.setMq(messageQueue);
                consumeMessageContext.setMsgList(msgs);
                consumeMessageContext.setSuccess(false);
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
            }

            long beginTimestamp = System.currentTimeMillis();
            boolean hasException = false;
            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
            try {
                // 设置 msg 的 CONSUME_START_TIME
                if (msgs != null && !msgs.isEmpty()) {
                    for (MessageExt msg : msgs) {
                        MessageAccessor.setConsumeStartTimeStamp(msg, String.valueOf(System.currentTimeMillis()));
                    }
                }

                /**
                 * 执行具体的消息消费，调用应用程序消息监昕器的 consumeMessage 方法，进入到具体的消息消费业务逻辑，返回该批消息的消费结果
                 * 最终将返回 CONSUME_SUCCESS （消费成功）或 RECONSUME_LATER （需要重新消费）
                 */
                status = listener.consumeMessage(Collections.unmodifiableList(msgs), context);
            } catch (Throwable e) {
                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
                    RemotingHelper.exceptionSimpleDesc(e),
                    ConsumeMessageConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue);
                hasException = true;
            }

            // 消费耗时
            long consumeRT = System.currentTimeMillis() - beginTimestamp;

            // 消费status为空
            if (null == status) {
                if (hasException) {
                    returnType = ConsumeReturnType.EXCEPTION;
                } else {
                    returnType = ConsumeReturnType.RETURNNULL;
                }
            }
            // 消费耗时 大于 默认的 15分钟超时时间（这么说，不会进行消费超时的中断？？）
            else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                returnType = ConsumeReturnType.TIME_OUT;
            }
            // 需要重新消费  ConsumeReturnType.FAILED
            else if (ConsumeConcurrentlyStatus.RECONSUME_LATER == status) {
                returnType = ConsumeReturnType.FAILED;
            }
            // 消费成功
            else if (ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status) {
                returnType = ConsumeReturnType.SUCCESS;
            }

            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
            }

            if (null == status) {
                log.warn("consumeMessage return null, Group: {} Msgs: {} MQ: {}",
                    ConsumeMessageConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue);
                status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }

            // 如果有，执行消费后Hook
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.setStatus(status.toString());
                consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
            }

            ConsumeMessageConcurrentlyService.this.getConsumerStatsManager()
                .incConsumeRT(ConsumeMessageConcurrentlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);

            /**
             * 执行业务消息消费后，在处理结果前再次验证一 下 ProcessQueue 的 isDroped状态值
             * 如果设置为true，将不对结果进行处理，也就是说如果在消息消费过程中，
             * 如果由于由新的消费者加入或原先的消费者出现宕机导致原先分给消费者的队列在负载之后分配给别的消费者，那么在应用程序的角度来看的话，消息会被重复消费
             */
            if (!processQueue.isDropped()) {
                // 处理消费结果
                ConsumeMessageConcurrentlyService.this.processConsumeResult(status, context, this);
            }
            else {
                // 告警，processQueue被drop而导致未处理消费结果，可能会导致重复消费
                log.warn("processQueue is dropped without process consume result. messageQueue={}, msgs={}", messageQueue, msgs);
            }
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

    }
}
