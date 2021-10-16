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

package com.alibaba.polardbx.net.handler;

public interface LoadDataHandler {

    /**
     * 根据load data sql, 初始化loadDataHandle
     */
    void open(String sql);

    /**
     * 接收到load data最后的一个包，即end包时调用
     */
    void end();

    /**
     * 释放load Data Handle占用的资源
     */
    void close();

    /**
     * 接收到load data的packet，非end包时调用
     */
    void putData(byte[] data);

    /**
     * 获取最新的packetId，packetId应严格递增，以防出现串包问题
     */
    byte getPacketId();

    /**
     * 设置packetId，packetId应严格递增，以防出现串包问题
     */
    void setPacketId(byte loadDataPacketId);

    /**
     * 是否处于接收load data数据流的状态
     */
    boolean isStart();

    boolean isBlocked();

    boolean isFull();

    Throwable throwError();
}

