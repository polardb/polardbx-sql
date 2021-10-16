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

package com.alibaba.polardbx.optimizer.utils;

/**
 * 并行策略
 * <p>
 * Created by chuanqin on 17/11/16.
 */
public enum QueryConcurrencyPolicy {
    /**
     * 串行执行
     */
    SEQUENTIAL,

    /**
     * 分表级并发
     */
    CONCURRENT,

    /**
     * 库（Group）级并发
     */
    GROUP_CONCURRENT_BLOCK,

    /**
     * 实例级并发
     */
    INSTANCE_CONCURRENT,

    /**
     * 第一个请求先执行，其他请求并发（用于广播表多写）
     */
    FIRST_THEN_CONCURRENT;
}
