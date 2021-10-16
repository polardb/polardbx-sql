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

package com.alibaba.polardbx.net;

import com.alibaba.polardbx.net.buffer.BufferPool;
import com.alibaba.polardbx.net.handler.CommandCount;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author xianmao.hexm
 */
public final class NIOProcessor {

    private static final Logger logger = LoggerFactory.getLogger(NIOProcessor.class);
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024 * 16;
    private static final int DEFAULT_BUFFER_CHUNK_SIZE = 4096;

    private int index;
    private final String name;
    private final NIOReactor reactor;
    private final BufferPool bufferPool;
    private final ServerThreadPool handler;
    private final ConcurrentMap<Long, FrontendConnection> frontends;
    private final CommandCount commands;
    private long netInBytes;
    private long netOutBytes;

    private LinkedBlockingQueue<FrontendConnection> holdingOnQueue = new LinkedBlockingQueue<FrontendConnection>();

    public NIOProcessor(int index, String name, int handler) throws IOException {
        this(index, name,
            DEFAULT_BUFFER_SIZE,
            DEFAULT_BUFFER_CHUNK_SIZE,
            handler);
    }

    public NIOProcessor(int index, String name, int buffer, int chunk, int handler) throws IOException {
        this.index = index;
        this.name = name;
        this.reactor = new NIOReactor(name);
        this.bufferPool = new BufferPool(buffer, chunk);
        // handler进行流控，出现单个schema或者ip链接过大时，拒绝链接
        this.handler = (handler > 0) ? ExecutorUtil.create(name + "-H", handler) : null;
        this.frontends = new ConcurrentHashMap<Long, FrontendConnection>();
        this.commands = new CommandCount();
    }

    public NIOProcessor(int index, String name, ServerThreadPool handler) throws IOException {
        this.index = index;
        this.name = name;
        this.reactor = new NIOReactor(name);
        this.bufferPool = new BufferPool(DEFAULT_BUFFER_SIZE, DEFAULT_BUFFER_CHUNK_SIZE);
        this.handler = handler;
        this.frontends = new ConcurrentHashMap<Long, FrontendConnection>();
        this.commands = new CommandCount();
    }

    public String getName() {
        return name;
    }

    public BufferPool getBufferPool() {
        return bufferPool;
    }

    public int getRegisterQueueSize() {
        return reactor.getRegisterQueue().size();
    }

    public int getWriteQueueSize() {
        return reactor.getWriteQueue().size();
    }

    public ServerThreadPool getHandler() {
        return handler;
    }

    public void startup() {
        reactor.startup();
    }

    public void postRegister(NIOConnection c) {
        reactor.postRegister(c);
    }

    public void postWrite(NIOConnection c) {
        reactor.postWrite(c);
    }

    public CommandCount getCommands() {
        return commands;
    }

    public long getNetInBytes() {
        return netInBytes;
    }

    public void addNetInBytes(long bytes) {
        netInBytes += bytes;
    }

    public long getNetOutBytes() {
        return netOutBytes;
    }

    public void addNetOutBytes(long bytes) {
        netOutBytes += bytes;
    }

    public long getReactCount() {
        return reactor.getReactCount();
    }

    public void addFrontend(FrontendConnection c) {
        frontends.put(c.getId(), c);
    }

    public int getIndex() {
        return index;
    }

    public ConcurrentMap<Long, FrontendConnection> getFrontends() {
        return frontends;
    }

    /**
     * 定时执行该方法，回收部分资源。
     */
    public void check() {
        frontendCheck();
    }

    // 前端连接检查
    private void frontendCheck() {
        Iterator<Entry<Long, FrontendConnection>> it = frontends.entrySet().iterator();
        while (it.hasNext()) {
            FrontendConnection c = it.next().getValue();

            // 删除空连接
            if (c == null) {
                it.remove();
                continue;
            }

            // 清理已关闭连接，否则空闲检查。
            if (c.isClosed()) {
                it.remove();
                c.cleanup();
            } else {
                c.idleCheck();
            }
        }
    }

    public LinkedBlockingQueue<FrontendConnection> getHoldingOnQueue() {
        return holdingOnQueue;
    }
}
