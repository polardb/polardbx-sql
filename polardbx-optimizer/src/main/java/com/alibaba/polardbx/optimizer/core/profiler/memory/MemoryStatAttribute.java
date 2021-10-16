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

package com.alibaba.polardbx.optimizer.core.profiler.memory;

import com.alibaba.polardbx.optimizer.memory.MemorySetting;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class MemoryStatAttribute {

    // =======PoolName========
    public static final String PARSER_POOL = "Parser";
    public static final String PLANNER_POOL = "Planner";
    public static final String PLAN_EXECUTOR_POOL = "PlanExecutor";
    public static final String PLAN_BUILDER_POOL = "DistributedPlanBuilder";
    public static final String OPERATOR_TMP_TABLE_POOL = "OperatorTmpTable";
    public static final String SCALAR_SUBQUERY_POOL = "ScalarSubQuery";
    public static final String APPLY_SUBQUERY_POOL = "ApplySubQuery";

    // Use to init general pool for StmtMemPool
    public static final List<String> normalPoolTypeNameList = new ArrayList<String>();

    static {
        normalPoolTypeNameList.add(MemorySetting.ENABLE_PARSER_POOL == true ? PARSER_POOL : null);
        normalPoolTypeNameList.add(MemorySetting.ENABLE_PLANNER_POOL == true ? PLANNER_POOL : null);
        normalPoolTypeNameList.add(MemorySetting.ENABLE_PLAN_EXECUTOR_POOL == true ? PLAN_EXECUTOR_POOL : null);
        normalPoolTypeNameList.add(MemorySetting.ENABLE_PLAN_BUILDER_POOL == true ? PLAN_BUILDER_POOL : null);
        normalPoolTypeNameList
            .add(MemorySetting.ENABLE_OPERATOR_TMP_TABLE_POOL == true ? OPERATOR_TMP_TABLE_POOL : null);
    }

    // ========AllocationId=========

    /**
     * memory allocation id for logical sql
     */
    public static final String PARSER_SQL = "SqlText";

    /**
     * memory allocation id for fast sql ast
     */
    public static final String PARSER_FASTAST = "FastAst";

    /**
     *
     */
    public static final String PARSER_FASTAST_PARAMS = "FastAstParams";

    /**
     * memory allocation id for calcite sql node
     */
    public static final String PARSER_SQLNODE = "LogicalAst";

    /**
     * memory allocation id for calcite rel node
     */
    public static final String OPTIMIZER_RELNODE = "LogicalPlan";

    /**
     * memory allocation id for open executor node
     */
    public static final String POST_OPTIMIZER_CURSOR = "PhysicalCursor";

    /**
     * memory allocation id for all physical operators
     */
    public static final String PHYSICAL_OPERATOR = "PhysicalOperators";

    /**
     * memory allocation id for all physical cursors
     */
    public static final String PHYSICAL_EXECUTOR = "PhysicalExecutors";

    /**
     * memory allocation id for the tmp table of select vals of insert select
     */
    public static final String INSERT_SELECT_VALUES = "InsertSelectValues";

}
