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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;

/**
 * Queue consumption snapshot
 */
public class ProcessQueue {
    public final static long REBALANCE_LOCK_MAX_LIVE_TIME =
        Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockMaxLiveTime", "30000"));
    public final static long REBALANCE_LOCK_INTERVAL = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockInterval", "20000"));
    private final static long PULL_MAX_IDLE_TIME = Long.parseLong(System.getProperty("rocketmq.client.pull.pullMaxIdleTime", "120000"));
    private final InternalLogger log = ClientLogger.getLog();

    // ProcessQueue 是与一个 MessageQueue 对应的，多个线程会共同操作，故需要读写锁
    private final ReadWriteLock lockTreeMap = new ReentrantReadWriteLock();

    // 拉取消息后，存储在 msgTreeMap
    private final TreeMap<Long, MessageExt> msgTreeMap = new TreeMap<Long, MessageExt>();

    private final AtomicLong msgCount = new AtomicLong();
    private final AtomicLong msgSize = new AtomicLong();
    private final Lock lockConsume = new ReentrantLock();
    /**
     * A subset of msgTreeMap, will only be used when orderly consume
     */
    private final TreeMap<Long, MessageExt> consumingMsgOrderlyTreeMap = new TreeMap<Long, MessageExt>();
    private final AtomicLong tryUnlockTimes = new AtomicLong(0);
    private volatile long queueOffsetMax = 0L;
    private volatile boolean dropped = false;
    private volatile long lastPullTimestamp = System.currentTimeMillis();
    private volatile long lastConsumeTimestamp = System.currentTimeMillis();
    private volatile boolean locked = false;
    private volatile long lastLockTimestamp = System.currentTimeMillis();
    private volatile boolean consuming = false;

    // broker端还有多少消息堆积，未消费（根据 broker端maxOffset - 得到消息的offset）
    private volatile long msgAccCnt = 0;


    public boolean isLockExpired() {
        return (System.currentTimeMillis() - this.lastLockTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;
    }

    public boolean isPullExpired() {
        return (System.currentTimeMillis() - this.lastPullTimestamp) > PULL_MAX_IDLE_TIME;
    }

    /**
     * 清除过期消息，定时任务调用
     */
    public void cleanExpiredMsg(DefaultMQPushConsumer pushConsumer) {
        // 如果是顺序消费，不处理
        if (pushConsumer.getDefaultMQPushConsumerImpl().isConsumeOrderly()) {
            return;
        }

        // 循环次数，至多 16 次
        // 每次找 msgTreeMap 第一个元素，如果超时，则发回，并删除
        int loop = msgTreeMap.size() < 16 ? msgTreeMap.size() : 16;
        for (int i = 0; i < loop; i++) {
            MessageExt msg = null;
            try {
                // 读锁
                this.lockTreeMap.readLock().lockInterruptibly();
                try {
                    // msgTreeMap中第一个元素如果达到了 ConsumeTimeout 消费超时（默认15分钟），将其 sendMessageBack
                    if (!msgTreeMap.isEmpty() &&
                            System.currentTimeMillis() - Long.parseLong(MessageAccessor.getConsumeStartTimeStamp(msgTreeMap.firstEntry().getValue())) > pushConsumer.getConsumeTimeout() * 60 * 1000) {
                        msg = msgTreeMap.firstEntry().getValue();
                    }
                    else {
                        break;
                    }
                } finally {
                    this.lockTreeMap.readLock().unlock();
                }
            } catch (InterruptedException e) {
                log.error("getExpiredMsg exception", e);
            }

            try {
                // 将找到的 ProcessQueue 中的超时未消费消息，发回，delay级别 3
                pushConsumer.sendMessageBack(msg, 3);
                log.info("send expire msg back. topic={}, msgId={}, storeHost={}, queueId={}, queueOffset={}", msg.getTopic(), msg.getMsgId(), msg.getStoreHost(), msg.getQueueId(), msg.getQueueOffset());

                try {
                    // 写锁
                    this.lockTreeMap.writeLock().lockInterruptibly();
                    try {
                        // 判断 msg 还是 msgTreeMap 的第一条消息？？
                        // 可能在上面判断超时后，msg 又被消费了？？
                        if (!msgTreeMap.isEmpty() && msg.getQueueOffset() == msgTreeMap.firstKey()) {
                            try {
                                // 删除这条msg
                                removeMessage(Collections.singletonList(msg));
                            } catch (Exception e) {
                                log.error("send expired msg exception", e);
                            }
                        }
                    } finally {
                        this.lockTreeMap.writeLock().unlock();
                    }
                } catch (InterruptedException e) {
                    log.error("getExpiredMsg exception", e);
                }
            } catch (Exception e) {
                log.error("send expired msg exception", e);
            }
        }
    }


    /**
     * 添加消息到 msgTreeMap
     */
    public boolean putMessage(final List<MessageExt> msgs) {
        boolean dispatchToConsume = false;
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                int validMsgCnt = 0;
                for (MessageExt msg : msgs) {
                    // 以消息在MessageQueue的偏移量为key，消息本身为value
                    MessageExt old = msgTreeMap.put(msg.getQueueOffset(), msg);

                    // old==null，说明 key 不存在
                    if (null == old) {
                        validMsgCnt++;
                        this.queueOffsetMax = msg.getQueueOffset(); // 说明参数中拉取回来的msgs是有顺序的，validate的消息的offset就是当前ProcessQueue的最大offset
                        msgSize.addAndGet(msg.getBody().length);
                    }
                }
                msgCount.addAndGet(validMsgCnt);

                if (!msgTreeMap.isEmpty() && !this.consuming) {
                    dispatchToConsume = true; // 顺序消费 ConsumeMessageOrderlyService 中会用到
                    this.consuming = true;
                }

                if (!msgs.isEmpty()) {
                    MessageExt messageExt = msgs.get(msgs.size() - 1);
                    String property = messageExt.getProperty(MessageConst.PROPERTY_MAX_OFFSET); // broker端MessageQueue的最大偏移量
                    if (property != null) {
                        long accTotal = Long.parseLong(property) - messageExt.getQueueOffset();
                        if (accTotal > 0) {
                            this.msgAccCnt = accTotal;  // 消息堆积数量？？还有多少条消息没消费？？
                        }
                    }
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }

        return dispatchToConsume;
    }


    /**
     * msgTreeMap中元素的key的最大差值（key是MessageQueue的offset，也就是offset的最大差值）
     * @return
     */
    public long getMaxSpan() {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    return this.msgTreeMap.lastKey() - this.msgTreeMap.firstKey();
                }
            } finally {
                this.lockTreeMap.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getMaxSpan exception", e);
        }

        return 0;
    }


    /**
     * 从msgTreeMap删除消息
     * 返回的是 删除参数中的msgs后，msgTreeMap的firstKey，即msgTreeMap当前消息的最小偏移量
     * @param msgs
     * @return
     */
    public long removeMessage(final List<MessageExt> msgs) {
        long result = -1;
        final long now = System.currentTimeMillis();
        try {
            // 写锁
            this.lockTreeMap.writeLock().lockInterruptibly();
            this.lastConsumeTimestamp = now;
            try {
                if (!msgTreeMap.isEmpty()) {
                    result = this.queueOffsetMax + 1;
                    int removedCnt = 0;
                    for (MessageExt msg : msgs) {
                        MessageExt prev = msgTreeMap.remove(msg.getQueueOffset());
                        if (prev != null) {
                            removedCnt--;
                            msgSize.addAndGet(0 - msg.getBody().length);
                        }
                    }
                    msgCount.addAndGet(removedCnt);

                    if (!msgTreeMap.isEmpty()) {
                        result = msgTreeMap.firstKey();
                    }
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (Throwable t) {
            log.error("removeMessage exception", t);
        }

        return result;
    }

    public TreeMap<Long, MessageExt> getMsgTreeMap() {
        return msgTreeMap;
    }

    public AtomicLong getMsgCount() {
        return msgCount;
    }

    public AtomicLong getMsgSize() {
        return msgSize;
    }

    public boolean isDropped() {
        return dropped;
    }

    public void setDropped(boolean dropped) {
        this.dropped = dropped;
    }

    public boolean isLocked() {
        return locked;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    public void rollback() {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.putAll(this.consumingMsgOrderlyTreeMap);
                this.consumingMsgOrderlyTreeMap.clear();
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    /**
     * 顺序消费成功时会commit
     * @return
     */
    public long commit() {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                Long offset = this.consumingMsgOrderlyTreeMap.lastKey();

                // 消息数量减去consumingMsgOrderlyTreeMap.size()
                msgCount.addAndGet(0 - this.consumingMsgOrderlyTreeMap.size());
                // 消息size依次减去msg.getBody().length
                for (MessageExt msg : this.consumingMsgOrderlyTreeMap.values()) {
                    msgSize.addAndGet(0 - msg.getBody().length);
                }
                // 清除consumingMsgOrderlyTreeMap
                this.consumingMsgOrderlyTreeMap.clear();

                // 返回offset是 consumingMsgOrderlyTreeMap.lastKey() + 1
                if (offset != null) {
                    return offset + 1;
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("commit exception", e);
        }

        return -1;
    }

    public void makeMessageToCosumeAgain(List<MessageExt> msgs) {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                for (MessageExt msg : msgs) {
                    this.consumingMsgOrderlyTreeMap.remove(msg.getQueueOffset());
                    this.msgTreeMap.put(msg.getQueueOffset(), msg);
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("makeMessageToCosumeAgain exception", e);
        }
    }

    public List<MessageExt> takeMessags(final int batchSize) {
        List<MessageExt> result = new ArrayList<MessageExt>(batchSize);
        final long now = System.currentTimeMillis();
        try {
            // 写锁
            this.lockTreeMap.writeLock().lockInterruptibly();
            this.lastConsumeTimestamp = now;
            try {
                // 不以 consuming 判断，只要 msgTreeMap 不为空就可以takeMessags
                if (!this.msgTreeMap.isEmpty()) {
                    // 循环 batchSize 次
                    for (int i = 0; i < batchSize; i++) {
                        Map.Entry<Long, MessageExt> entry = this.msgTreeMap.pollFirstEntry(); // 取出 First Entry
                        if (entry != null) {
                            result.add(entry.getValue()); // 返回的结果集
                            consumingMsgOrderlyTreeMap.put(entry.getKey(), entry.getValue()); // 为了顺序消费，msgTreeMap的子集
                        } else {
                            break;
                        }
                    }
                }

                if (result.isEmpty()) {
                    consuming = false;
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("take Messages exception", e);
        }

        return result;
    }

    public boolean hasTempMessage() {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();
            try {
                return !this.msgTreeMap.isEmpty();
            } finally {
                this.lockTreeMap.readLock().unlock();
            }
        } catch (InterruptedException e) {
        }

        return true;
    }

    public void clear() {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.clear();
                this.consumingMsgOrderlyTreeMap.clear();
                this.msgCount.set(0);
                this.msgSize.set(0);
                this.queueOffsetMax = 0L;
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    public long getLastLockTimestamp() {
        return lastLockTimestamp;
    }

    public void setLastLockTimestamp(long lastLockTimestamp) {
        this.lastLockTimestamp = lastLockTimestamp;
    }

    public Lock getLockConsume() {
        return lockConsume;
    }

    public long getLastPullTimestamp() {
        return lastPullTimestamp;
    }

    public void setLastPullTimestamp(long lastPullTimestamp) {
        this.lastPullTimestamp = lastPullTimestamp;
    }

    public long getMsgAccCnt() {
        return msgAccCnt;
    }

    public void setMsgAccCnt(long msgAccCnt) {
        this.msgAccCnt = msgAccCnt;
    }

    public long getTryUnlockTimes() {
        return this.tryUnlockTimes.get();
    }

    public void incTryUnlockTimes() {
        this.tryUnlockTimes.incrementAndGet();
    }

    public void fillProcessQueueInfo(final ProcessQueueInfo info) {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();

            if (!this.msgTreeMap.isEmpty()) {
                info.setCachedMsgMinOffset(this.msgTreeMap.firstKey());
                info.setCachedMsgMaxOffset(this.msgTreeMap.lastKey());
                info.setCachedMsgCount(this.msgTreeMap.size());
                info.setCachedMsgSizeInMiB((int) (this.msgSize.get() / (1024 * 1024)));
            }

            if (!this.consumingMsgOrderlyTreeMap.isEmpty()) {
                info.setTransactionMsgMinOffset(this.consumingMsgOrderlyTreeMap.firstKey());
                info.setTransactionMsgMaxOffset(this.consumingMsgOrderlyTreeMap.lastKey());
                info.setTransactionMsgCount(this.consumingMsgOrderlyTreeMap.size());
            }

            info.setLocked(this.locked);
            info.setTryUnlockTimes(this.tryUnlockTimes.get());
            info.setLastLockTimestamp(this.lastLockTimestamp);

            info.setDroped(this.dropped);
            info.setLastPullTimestamp(this.lastPullTimestamp);
            info.setLastConsumeTimestamp(this.lastConsumeTimestamp);
        } catch (Exception e) {
        } finally {
            this.lockTreeMap.readLock().unlock();
        }
    }

    public long getLastConsumeTimestamp() {
        return lastConsumeTimestamp;
    }

    public void setLastConsumeTimestamp(long lastConsumeTimestamp) {
        this.lastConsumeTimestamp = lastConsumeTimestamp;
    }

}
