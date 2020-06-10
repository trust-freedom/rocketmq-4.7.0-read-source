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

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

public class RebalancePushImpl extends RebalanceImpl {
    private final static long UNLOCK_DELAY_TIME_MILLS = Long.parseLong(System.getProperty("rocketmq.client.unlockDelayTimeMills", "20000"));
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

    public RebalancePushImpl(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        this(null, null, null, null, defaultMQPushConsumerImpl);
    }

    public RebalancePushImpl(String consumerGroup, MessageModel messageModel,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy,
        MQClientInstance mQClientFactory, DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        super(consumerGroup, messageModel, allocateMessageQueueStrategy, mQClientFactory);
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
    }

    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
        /**
         * When rebalance result changed, should update subscription's version to notify broker.
         * Fix: inconsistency subscription may lead to consumer miss messages.
         */
        SubscriptionData subscriptionData = this.subscriptionInner.get(topic);
        long newVersion = System.currentTimeMillis();
        log.info("{} Rebalance changed, also update version: {}, {}", topic, subscriptionData.getSubVersion(), newVersion);
        subscriptionData.setSubVersion(newVersion);

        int currentQueueCount = this.processQueueTable.size();
        if (currentQueueCount != 0) {
            int pullThresholdForTopic = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdForTopic();
            if (pullThresholdForTopic != -1) {
                int newVal = Math.max(1, pullThresholdForTopic / currentQueueCount);
                log.info("The pullThresholdForQueue is changed from {} to {}",
                    this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdForQueue(), newVal);
                this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().setPullThresholdForQueue(newVal);
            }

            int pullThresholdSizeForTopic = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdSizeForTopic();
            if (pullThresholdSizeForTopic != -1) {
                int newVal = Math.max(1, pullThresholdSizeForTopic / currentQueueCount);
                log.info("The pullThresholdSizeForQueue is changed from {} to {}",
                    this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdSizeForQueue(), newVal);
                this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().setPullThresholdSizeForQueue(newVal);
            }
        }

        // notify broker
        this.getmQClientFactory().sendHeartbeatToAllBrokerWithLock();
    }

    /**
     * 将不必要的Queue从内存缓存中移除，并持久化（集群模式：向Broker）当前Queue的消费偏移量
     * @param mq
     * @param pq
     * @return
     */
    @Override
    public boolean removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq) {
        // 集群模式：RemoteBrokerOffsetStore
        // 广播模式：LocalFileOffsetStore
        this.defaultMQPushConsumerImpl.getOffsetStore().persist(mq);  // 集群模式：向Broker更新消费偏移量
        this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq); // 集群模式：内存维护的 ConcurrentMap<MessageQueue, AtomicLong> offsetTable 删除当前Queue

        // 顺序消费 && 集群消费
        if (this.defaultMQPushConsumerImpl.isConsumeOrderly()
            && MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
            try {
                if (pq.getLockConsume().tryLock(1000, TimeUnit.MILLISECONDS)) {
                    try {
                        return this.unlockDelay(mq, pq);
                    } finally {
                        pq.getLockConsume().unlock();
                    }
                } else {
                    log.warn("[WRONG]mq is consuming, so can not unlock it, {}. maybe hanged for a while, {}",
                        mq,
                        pq.getTryUnlockTimes());

                    pq.incTryUnlockTimes();
                }
            } catch (Exception e) {
                log.error("removeUnnecessaryMessageQueue Exception", e);
            }

            return false;
        }
        return true;
    }

    private boolean unlockDelay(final MessageQueue mq, final ProcessQueue pq) {

        if (pq.hasTempMessage()) {
            log.info("[{}]unlockDelay, begin {} ", mq.hashCode(), mq);
            this.defaultMQPushConsumerImpl.getmQClientFactory().getScheduledExecutorService().schedule(new Runnable() {
                @Override
                public void run() {
                    log.info("[{}]unlockDelay, execute at once {}", mq.hashCode(), mq);
                    RebalancePushImpl.this.unlock(mq, true);
                }
            }, UNLOCK_DELAY_TIME_MILLS, TimeUnit.MILLISECONDS);
        } else {
            this.unlock(mq, true);
        }
        return true;
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_PASSIVELY;
    }

    @Override
    public void removeDirtyOffset(final MessageQueue mq) {
        this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);
    }

    /**
     * 计算MessageQueue的拉取偏移量
     * @param mq
     * @return
     */
    @Override
    public long computePullFromWhere(MessageQueue mq) {
        long result = -1;
        // 创建消费者时设置的"从哪儿开始消费"，默认 CONSUME_FROM_LAST_OFFSET  从队列最新偏移量开始消费
        final ConsumeFromWhere consumeFromWhere = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeFromWhere();
        final OffsetStore offsetStore = this.defaultMQPushConsumerImpl.getOffsetStore();
        switch (consumeFromWhere) {
            case CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST:
            case CONSUME_FROM_MIN_OFFSET:
            case CONSUME_FROM_MAX_OFFSET:
            case CONSUME_FROM_LAST_OFFSET: {  // 从队列最新偏移量开始消费（集群模式：从Broker获取）
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE); // RemoteBrokerOffsetStore + READ_FROM_STORE：从Broker获取offset

                /**
                 * 如果返回的偏移量大于等于0，则直接使用该offset
                 * 大于等于0，表示查询到有效的消息消费进度，从该有效进度开始消费
                 * 但我们要特别留意lastOffset为0是什么场景，因为返回0，并不会执行CONSUME_FROM_LAST_OFFSET（语义）
                 *
                 * Broker端：ConsumerManageProcessor#queryConsumerOffset()  （ConsumeQueue存储目录下名字排序最小的文件 就是 MinOffsetInQueue）
                 * 如果 store/config/offset.json 中有数据，就根据数据返回offset
                 * 如果 store/config/offset.json 中没有数据，计算 ConsumeQueue.getMinOffsetInQueue() 最小偏移量
                 *   如果 MinOffsetInQueue <=0 且 最小偏移量对应的消息存储在内存中而不是存在磁盘中，则返回偏移量0，这就会从头开始消费（不满足 CONSUME_FROM_LAST_OFFSET 语义）
                 *   其它情况，lastOffset 返回 -1，普通Topic，从MessageQueue的最大偏移量开始（满足 CONSUME_FROM_LAST_OFFSET 语义）
                 */
                if (lastOffset >= 0) {
                    result = lastOffset;
                }
                // First start,no offset
                /**
                 * 如果lastOffset为-1，表示当前并未存储其有效偏移量，可以理解为第一次消费
                 * 如果是消费组重试主题，从重试队列偏移量为0开始消费
                 * 如果是普通主题，则从队列当前的最大的有效偏移量开始消费，即CONSUME_FROM_LAST_OFFSET语义的实现
                 */
                else if (-1 == lastOffset) {
                    // %RETRY%，从0开始
                    if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        result = 0L;
                    }
                    // 普通Topic，从MessageQueue的最大偏移量开始
                    else {
                        try {
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                        } catch (MQClientException e) {
                            result = -1;
                        }
                    }
                }
                // 有错误
                else {
                    result = -1;
                }
                break;
            }
            case CONSUME_FROM_FIRST_OFFSET: { // 从头开始消费
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE); // RemoteBrokerOffsetStore + READ_FROM_STORE：从Broker获取offset
                // 如果从Broker获取了lastOffset>0，直接使用
                if (lastOffset >= 0) {
                    result = lastOffset;
                }
                // 如果 lastOffset == -1，直接从0开始【不同点】
                // 可能 MQBrokerException No offset in broker
                else if (-1 == lastOffset) {
                    result = 0L;
                }
                // 有错误
                else {
                    result = -1;
                }
                break;
            }
            case CONSUME_FROM_TIMESTAMP: { // 从消费者启动的时间戳对应的消费进度开始消费（默认是消费者启动之前的半小时）
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);

                if (lastOffset >= 0) {
                    result = lastOffset;
                }
                else if (-1 == lastOffset) {
                    // %RETRY%，从MessageQueue的最大偏移量开始
                    if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        try {
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                        } catch (MQClientException e) {
                            result = -1;
                        }
                    }
                    // 普通Topic，根据消费者启动时间戳去查找MessageQueue的拉取偏移量
                    else {
                        try {
                            long timestamp = UtilAll.parseDate(this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeTimestamp(),
                                UtilAll.YYYYMMDDHHMMSS).getTime();
                            result = this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
                        } catch (MQClientException e) {
                            result = -1;
                        }
                    }
                }
                // 有错误
                else {
                    result = -1;
                }
                break;
            }

            default:
                break;
        }

        return result;
    }

    @Override
    public void dispatchPullRequest(List<PullRequest> pullRequestList) {
        for (PullRequest pullRequest : pullRequestList) {
            this.defaultMQPushConsumerImpl.executePullRequestImmediately(pullRequest);
            log.info("doRebalance, {}, add a new pull request {}", consumerGroup, pullRequest);
        }
    }
}
