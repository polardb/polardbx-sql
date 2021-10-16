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
import com.alibaba.polardbx.rule.enumerator.EnumerationFailedException;

/**
 * 如果不能进行枚举，那么就是用默认的枚举器 默认枚举器只支持comparativeOr条件，以及等于的关系。不支持大于小于等一系列关系。
 *
 * @author shenxun
 */
public class DefaultEnumerator extends AbstractCloseIntervalFieldsEnumeratorHandler {

    public void mergeFeildOfDefinitionInCloseInterval(Comparative from, Comparative to, Set<Object> retValue,
                                                      Integer cumulativeTimes, Comparable<?> atomIncrValue) {
        throw new EnumerationFailedException("default enumerator not support traversal");

    }

    public void processAllPassableFields(Comparative source, Set<Object> retValue, Integer cumulativeTimes,
                                         Comparable<?> atomIncrValue) {
        throw new EnumerationFailedException("default enumerator not support traversal, not support > < >= <=");
    }
}
