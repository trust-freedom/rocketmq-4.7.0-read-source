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
     * ������Ҫ��Queue���ڴ滺�����Ƴ������־û�����Ⱥģʽ����Broker����ǰQueue������ƫ����
     * @param mq
     * @param pq
     * @return
     */
    @Override
    public boolean removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq) {
        // ��Ⱥģʽ��RemoteBrokerOffsetStore
        // �㲥ģʽ��LocalFileOffsetStore
        this.defaultMQPushConsumerImpl.getOffsetStore().persist(mq);  // ��Ⱥģʽ����Broker��������ƫ����
        this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq); // ��Ⱥģʽ���ڴ�ά���� ConcurrentMap<MessageQueue, AtomicLong> offsetTable ɾ����ǰQueue

        // ˳������ && ��Ⱥ����
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
     * ����MessageQueue����ȡƫ����
     * @param mq
     * @return
     */
    @Override
    public long computePullFromWhere(MessageQueue mq) {
        long result = -1;
        // ����������ʱ���õ�"���Ķ���ʼ����"��Ĭ�� CONSUME_FROM_LAST_OFFSET  �Ӷ�������ƫ������ʼ����
        final ConsumeFromWhere consumeFromWhere = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeFromWhere();
        final OffsetStore offsetStore = this.defaultMQPushConsumerImpl.getOffsetStore();
        switch (consumeFromWhere) {
            case CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST:
            case CONSUME_FROM_MIN_OFFSET:
            case CONSUME_FROM_MAX_OFFSET:
            case CONSUME_FROM_LAST_OFFSET: {  // �Ӷ�������ƫ������ʼ���ѣ���Ⱥģʽ����Broker��ȡ��
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE); // RemoteBrokerOffsetStore + READ_FROM_STORE����Broker��ȡoffset

                /**
                 * ������ص�ƫ�������ڵ���0����ֱ��ʹ�ø�offset
                 * ���ڵ���0����ʾ��ѯ����Ч����Ϣ���ѽ��ȣ��Ӹ���Ч���ȿ�ʼ����
                 * ������Ҫ�ر�����lastOffsetΪ0��ʲô��������Ϊ����0��������ִ��CONSUME_FROM_LAST_OFFSET�����壩
                 *
                 * Broker�ˣ�ConsumerManageProcessor#queryConsumerOffset()  ��ConsumeQueue�洢Ŀ¼������������С���ļ� ���� MinOffsetInQueue��
                 * ��� store/config/offset.json �������ݣ��͸������ݷ���offset
                 * ��� store/config/offset.json ��û�����ݣ����� ConsumeQueue.getMinOffsetInQueue() ��Сƫ����
                 *   ��� MinOffsetInQueue <=0 �� ��Сƫ������Ӧ����Ϣ�洢���ڴ��ж����Ǵ��ڴ����У��򷵻�ƫ����0����ͻ��ͷ��ʼ���ѣ������� CONSUME_FROM_LAST_OFFSET ���壩
                 *   ���������lastOffset ���� -1����ͨTopic����MessageQueue�����ƫ������ʼ������ CONSUME_FROM_LAST_OFFSET ���壩
                 */
                if (lastOffset >= 0) {
                    result = lastOffset;
                }
                // First start,no offset
                /**
                 * ���lastOffsetΪ-1����ʾ��ǰ��δ�洢����Чƫ�������������Ϊ��һ������
                 * ������������������⣬�����Զ���ƫ����Ϊ0��ʼ����
                 * �������ͨ���⣬��Ӷ��е�ǰ��������Чƫ������ʼ���ѣ���CONSUME_FROM_LAST_OFFSET�����ʵ��
                 */
                else if (-1 == lastOffset) {
                    // %RETRY%����0��ʼ
                    if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        result = 0L;
                    }
                    // ��ͨTopic����MessageQueue�����ƫ������ʼ
                    else {
                        try {
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                        } catch (MQClientException e) {
                            result = -1;
                        }
                    }
                }
                // �д���
                else {
                    result = -1;
                }
                break;
            }
            case CONSUME_FROM_FIRST_OFFSET: { // ��ͷ��ʼ����
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE); // RemoteBrokerOffsetStore + READ_FROM_STORE����Broker��ȡoffset
                // �����Broker��ȡ��lastOffset>0��ֱ��ʹ��
                if (lastOffset >= 0) {
                    result = lastOffset;
                }
                // ��� lastOffset == -1��ֱ�Ӵ�0��ʼ����ͬ�㡿
                // ���� MQBrokerException No offset in broker
                else if (-1 == lastOffset) {
                    result = 0L;
                }
                // �д���
                else {
                    result = -1;
                }
                break;
            }
            case CONSUME_FROM_TIMESTAMP: { // ��������������ʱ�����Ӧ�����ѽ��ȿ�ʼ���ѣ�Ĭ��������������֮ǰ�İ�Сʱ��
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);

                if (lastOffset >= 0) {
                    result = lastOffset;
                }
                else if (-1 == lastOffset) {
                    // %RETRY%����MessageQueue�����ƫ������ʼ
                    if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        try {
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                        } catch (MQClientException e) {
                            result = -1;
                        }
                    }
                    // ��ͨTopic����������������ʱ���ȥ����MessageQueue����ȡƫ����
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
                // �д���
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
