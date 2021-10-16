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

package com.alibaba.polardbx.rule.enumerator;

import java.util.Set;

/**
 * <pre>
 * 枚举器，提供了根据每步自增数 自增获取所有枚举值的操作
 * 主要是用于解决一个规则引擎中最大的难题
 * sql 条件 :id>100 and id < 200;
 * 这种条件是无法直接代入规则引擎中进行计算然后简单的取交集来计算的，具体请参见相关文档的介绍。
 *
 * 所以解决的方法就是把100~200之间的所有值都按照atomicIncreatementValue的设定值进行枚举。
 * 枚举出的值被放入set后返回给调用者。
 * </pre>
 *
 * @author shenxun
 */
public interface Enumerator {

    /**
     * 将#column,1,1024#结果进行展开，此时cumulativeTimes即为1024，atomicIncreatementValue即为1
     *
     * @param condition 条件
     * @param cumulativeTimes 值的个数，对于部分连续的函数来说，他完成一轮累加的次数是有限的，这里要求输入这个次数
     * @param atomicIncrementValue 引起值域发生最小变动的定义域原子增数值。ex:如果对于dayofweek这样的函数来说，引起值域
     * 发生变化的定义域的最小变动范围为1天。
     * @param needMergeValueInCloseInterval 是否需要对> < >= <= 进行计算。
     */
    public Set<Object> getEnumeratedValue(Comparable condition, Integer cumulativeTimes,
                                          Comparable<?> atomicIncrementValue, boolean needMergeValueInCloseInterval);
}
