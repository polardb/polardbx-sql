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

package com.alibaba.polardbx.optimizer.core.expression;

import java.util.List;

/**
 * 代表一个函数列，比如max(id)
 *
 * @author jianghang 2013-11-8 下午1:29:03
 * @since 5.0.0
 */
public interface IFunction<RT extends IFunction> extends ISelectable<RT> {

    public enum FunctionType {
        /**
         * 函数的操作面向一系列的值，并返回一个单一的值，可以理解为聚合函数
         */
        Aggregate,
        /**
         * 函数的操作面向某个单一的值，并返回基于输入值的一个单一的值，可以理解为转换函数
         */
        Scalar;
    }

    public String getFunctionName();

    public FunctionType getFunctionType();

    public IFunction setFunctionName(String funcName);

    public List getArgs();

    public RT setArgs(List objs);

    /**
     * 获取执行函数实现
     */
    public IExtraFunction getExtraFunction();

}
