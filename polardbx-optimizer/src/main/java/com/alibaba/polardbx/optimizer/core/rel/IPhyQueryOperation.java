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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.List;
import java.util.Map;

/**
 * @author lingce.ldm 2017-07-13 11:06
 */
public interface IPhyQueryOperation {

    /**
     * 获取参数化 SQL,表名与 SQL 中常量均被参数化
     */
    String getNativeSql();

    /**
     * 结合ExecutionContext的ParameterContext(即param)来
     * 获取执行 物理SQL所对应的 GroupKey 及其 该物理SQL 对应的参数化参数（包括参数化表名，是物理SQL级别的）
     *
     * @param param ExecutionContext的ParameterContext（逻辑SQL级别的参数化参数）
     */
    Pair<String, Map<Integer, ParameterContext>> getDbIndexAndParam(Map<Integer, ParameterContext> param,
                                                                    ExecutionContext executionContext);

    Pair<String, Map<Integer, ParameterContext>> getDbIndexAndParam(Map<Integer, ParameterContext> param,
                                                                    List<List<String>> phyTableNamesOutput,
                                                                    ExecutionContext executionContext);
}
