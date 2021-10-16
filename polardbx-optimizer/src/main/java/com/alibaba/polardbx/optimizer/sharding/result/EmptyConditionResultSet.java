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

package com.alibaba.polardbx.optimizer.sharding.result;

/**
 * @author chenmo.cm
 * @date 2020/2/25 10:03 上午
 */
class EmptyConditionResultSet implements ConditionResultSet {

    public static final EmptyConditionResultSet EMPTY = new EmptyConditionResultSet();

    public EmptyConditionResultSet(){
    }

    @Override
    public ConditionResult intersect() {
        return EmptyConditionResult.EMPTY;
    }

    @Override
    public ConditionResult first() {
        return EmptyConditionResult.EMPTY;
    }
}
