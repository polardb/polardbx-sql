/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.common.utils.logger.logback;

import ch.qos.logback.core.Appender;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.spi.AppenderAttachable;
import ch.qos.logback.core.spi.AppenderAttachableImpl;
import ch.qos.logback.core.util.InterruptUtil;
import com.alibaba.polardbx.common.properties.DynamicConfig;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class AsyncAppenderBase<E> extends UnsynchronizedAppenderBase<E> implements AppenderAttachable<E> {
    /**
     * The default maximum queue flush time allowed during appender stop. If the
     * worker takes longer than this time it will exit, discarding any remaining
     * items in the queue
     */
    public static final int DEFAULT_MAX_FLUSH_TIME = 1000;

    AppenderAttachableImpl<E> aai = new AppenderAttachableImpl<E>();
    BlockingQueue<E> blockingQueue;


    public static final int DEFAULT_QUEUE_SIZE = 256;
    int queueSize = DEFAULT_QUEUE_SIZE;

    int appenderCount = 0;

    static final int UNDEFINED = -1;
    int discardingThreshold = UNDEFINED;
    boolean neverBlock = false;
    boolean supportRemoteConsume = false;

    Worker worker = new Worker();

    int maxFlushTime = DEFAULT_MAX_FLUSH_TIME;

    protected Encoder<E> encoder;
    public long discardCount;

    /**
     * Is the eventObject passed as parameter discardable? The base class's implementation of this method always returns
     * 'false' but sub-classes may (and do) override this method.
     * <p/>
     * <p>Note that only if the buffer is nearly full are events discarded. Otherwise, when the buffer is "not full"
     * all events are logged.
     *
     * @return - true if the event can be discarded, false otherwise
     */
    protected boolean isDiscardable(E eventObject) {
        return false;
    }

    protected void preprocess(E eventObject) {
    }

    protected void loadAppender() {

    }

    protected void unLoadAppender() {

    }

    public Encoder<E> getEncoder() {
        return encoder;
    }

    public void setEncoder(Encoder<E> encoder) {
        this.encoder = encoder;
    }

    @Override
    public void start() {
        if (isStarted()) {
            return;
        }
        if (appenderCount == 0) {
            addError("No attached appenders found.");
            return;
        }
        if (queueSize < 1) {
            addError("Invalid queue size [" + queueSize + "]");
            return;
        }
        blockingQueue = new ArrayBlockingQueue<E>(queueSize);

        if (discardingThreshold == UNDEFINED) {
            discardingThreshold = queueSize / 5;
        }
        addInfo("Setting discardingThreshold to " + discardingThreshold);
        worker.setDaemon(true);
        worker.setName("AsyncAppender-Worker-" + getName());

        super.start();
        worker.start();
        loadAppender();
    }

    @Override
    public void stop() {
        if (!isStarted()) {
            return;
        }

        super.stop();

        worker.interrupt();

        unLoadAppender();

        InterruptUtil interruptUtil = new InterruptUtil(context);

        try {
            interruptUtil.maskInterruptFlag();

            worker.join(maxFlushTime);

            if (worker.isAlive()) {
                addWarn(
                    "Max queue flush timeout (" + maxFlushTime + " ms) exceeded. Approximately " + blockingQueue.size()
                        + " queued events were possibly discarded.");
            } else {
                addInfo("Queue flush finished successfully within timeout.");
            }

        } catch (InterruptedException e) {
            int remaining = blockingQueue.size();
            addError("Failed to join worker thread. " + remaining + " queued events may be discarded.", e);
        } finally {
            interruptUtil.unmaskInterruptFlag();
        }
    }

    @Override
    protected void append(E eventObject) {
        if (isQueueBelowDiscardingThreshold() && isDiscardable(eventObject)) {
            return;
        }
        preprocess(eventObject);
        put(eventObject);
    }

    private boolean isQueueBelowDiscardingThreshold() {
        return ((discardingThreshold > 0) && (blockingQueue.remainingCapacity() < discardingThreshold));
    }

    private void put(E eventObject) {
        if (neverBlock || (supportRemoteConsume && DynamicConfig.getInstance().enableRemoteConsumeLog())) {
            //allow event miss when it enable remote consume logs, or here maybe block!
            boolean ret = blockingQueue.offer(eventObject);
            if (!ret) {
                //there is a concurrency issue here, but it doesn't matter because it is mainly for performance.
                discardCount++;
            }
        } else {
            putUninterruptibly(eventObject);
        }
    }

    private void putUninterruptibly(E eventObject) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    blockingQueue.put(eventObject);
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public int getDiscardingThreshold() {
        return discardingThreshold;
    }

    public void setDiscardingThreshold(int discardingThreshold) {
        this.discardingThreshold = discardingThreshold;
    }

    public int getMaxFlushTime() {
        return maxFlushTime;
    }

    public void setMaxFlushTime(int maxFlushTime) {
        this.maxFlushTime = maxFlushTime;
    }

    public int getNumberOfElementsInQueue() {
        return blockingQueue.size();
    }

    public void setNeverBlock(boolean neverBlock) {
        this.neverBlock = neverBlock;
    }

    public boolean isNeverBlock() {
        return neverBlock;
    }

    public boolean isSupportRemoteConsume() {
        return supportRemoteConsume;
    }

    public void setSupportRemoteConsume(boolean supportRemoteConsume) {
        this.supportRemoteConsume = supportRemoteConsume;
    }

    /**
     * The remaining capacity available in the blocking queue.
     *
     * @return the remaining capacity
     * @see {@link java.util.concurrent.BlockingQueue#remainingCapacity()}
     */
    public int getRemainingCapacity() {
        return blockingQueue.remainingCapacity();
    }

    @Override
    public void addAppender(Appender<E> newAppender) {
        if (appenderCount == 0) {
            appenderCount++;
            addInfo("Attaching appender named [" + newAppender.getName() + "] to AsyncAppender.");
            aai.addAppender(newAppender);
        } else {
            addWarn("One and only one appender may be attached to AsyncAppender.");
            addWarn("Ignoring additional appender named [" + newAppender.getName() + "]");
        }
    }

    @Override
    public Iterator<Appender<E>> iteratorForAppenders() {
        return aai.iteratorForAppenders();
    }

    @Override
    public Appender<E> getAppender(String name) {
        return aai.getAppender(name);
    }

    @Override
    public boolean isAttached(Appender<E> eAppender) {
        return aai.isAttached(eAppender);
    }

    @Override
    public void detachAndStopAllAppenders() {
        aai.detachAndStopAllAppenders();
    }

    @Override
    public boolean detachAppender(Appender<E> eAppender) {
        return aai.detachAppender(eAppender);
    }

    @Override
    public boolean detachAppender(String name) {
        return aai.detachAppender(name);
    }

    public BlockingQueue<E> getBlockingQueue() {
        return blockingQueue;
    }

    class Worker extends Thread {

        @Override
        public void run() {
            AsyncAppenderBase<E> parent = AsyncAppenderBase.this;
            AppenderAttachableImpl<E> aai = parent.aai;

            while (parent.isStarted()) {
                try {
                    if (supportRemoteConsume && DynamicConfig.getInstance().enableRemoteConsumeLog()) {
                        Thread.sleep(1000);
                        continue;
                    }
                    E e = parent.blockingQueue.take();
                    aai.appendLoopOnAppenders(e);
                } catch (InterruptedException ie) {
                    break;
                } catch (Throwable ex) {
                    try {
                        System.out.println("LOGBACK: async thread exception.");
                        ex.printStackTrace(System.out);
                    } catch (Throwable t) {

                    }
                    continue;
                }
            }

            addInfo("Worker thread will flush remaining events before exiting. ");

            for (E e : parent.blockingQueue) {
                aai.appendLoopOnAppenders(e);
                parent.blockingQueue.remove(e);
            }

            aai.detachAndStopAllAppenders();
        }
    }

}
