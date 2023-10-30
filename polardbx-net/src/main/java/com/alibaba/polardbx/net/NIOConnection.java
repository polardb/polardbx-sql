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

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;

import java.io.IOException;
import java.nio.channels.Selector;

/**
 * @author xianmao.hexm
 */
public interface NIOConnection {

    /**
     * 注册网络事件
     */
    void register(Selector selector) throws IOException;

    /**
     * 从目标端读取数据
     */
    void read() throws IOException;

    /**
     * 向目标端写出一块缓存数据
     */
    void write(ByteBufferHolder buffer);

    /**
     * 基于处理器队列方式的数据写出
     */
    void writeByQueue() throws IOException;

    /**
     * 基于Selector事件方式的数据写出
     */
    void writeByEvent() throws IOException;

    /**
     * 处理数据
     */
    void handleData(byte[] data);

    /**
     * 处理错误
     */
    void handleError(ErrorCode errCode, Throwable t);

    /**
     * 关闭连接
     */
    boolean close();

}
