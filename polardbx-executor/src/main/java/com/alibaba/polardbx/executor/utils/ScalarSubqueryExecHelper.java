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

package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.IScalarSubqueryExecHelper;
import org.apache.calcite.rex.RexDynamicParam;

import java.util.List;

/**
 * Use to dynamic exec scalar subquery
 * @author chenghui.lch
 */
public class ScalarSubqueryExecHelper implements IScalarSubqueryExecHelper {

    private ScalarSubqueryExecHelper() {
    }

    private static ScalarSubqueryExecHelper instance = new ScalarSubqueryExecHelper();

    public static IScalarSubqueryExecHelper getInstance() {
        return instance;
    }

    @Override
    public void buildScalarSubqueryValue(List<RexDynamicParam> scalarList, ExecutionContext executionContext) {
        SubqueryUtils.buildScalarSubqueryValue(scalarList, executionContext);
        return;
    }
}
