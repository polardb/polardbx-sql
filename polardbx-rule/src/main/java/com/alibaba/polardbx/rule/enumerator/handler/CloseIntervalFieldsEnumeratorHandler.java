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

package com.alibaba.polardbx.rule.enumerator.handler;

import java.util.Set;

import com.alibaba.polardbx.common.model.sqljep.Comparative;

public interface CloseIntervalFieldsEnumeratorHandler {

    /**
     * 穷举出从source，根据自增value和自增次数Times，将结果写入retValue参数中
     */
    void processAllPassableFields(Comparative source, Set<Object> retValue, Integer cumulativeTimes,
                                  Comparable<?> atomIncrValue);

    /**
     * 穷举出从from到to中的所有值，根据自增value和自增次数Times，将结果写入retValue参数中
     */
    abstract void mergeFeildOfDefinitionInCloseInterval(Comparative from, Comparative to, Set<Object> retValue,
                                                        Integer cumulativeTimes, Comparable<?> atomIncrValue);

}
