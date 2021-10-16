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

package com.alibaba.polardbx.sequence;

import com.alibaba.polardbx.sequence.exception.SequenceException;

/**
 * 序列接口
 *
 * @author nianbing
 */
public interface Sequence {

    /**
     * 取得序列下一个值
     *
     * @return 返回序列下一个值
     */
    long nextValue() throws SequenceException;

    /**
     * 返回size大小后的值，比如针对batch拿到size大小的值，自己内存中顺序分配
     */
    long nextValue(int size) throws SequenceException;

    /**
     * 消耗掉当前内存中已分片的sequence
     */
    public boolean exhaustValue() throws SequenceException;
}
