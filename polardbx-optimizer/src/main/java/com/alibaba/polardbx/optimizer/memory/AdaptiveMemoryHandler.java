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

package com.alibaba.polardbx.optimizer.memory;

/**
 *
 **/
public interface AdaptiveMemoryHandler {

    /**
     * 释放AP的占用内容，如果支持spill，优先触发spill
     */
    void revokeReleaseMemory();

    /**
     * kill AP 占用内存最大的query
     */
    void killApQuery();

    /**
     * TP 限流
     */
    void limitTpRate();

    void limitApRate();

}
