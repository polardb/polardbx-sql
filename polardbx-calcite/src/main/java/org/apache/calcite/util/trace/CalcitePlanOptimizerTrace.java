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

package org.apache.calcite.util.trace;

import org.apache.calcite.sql.SqlExplainLevel;

/**
 * Created by chuanqin on 18/1/3.
 */
public class CalcitePlanOptimizerTrace {

    private static final ThreadLocal<Boolean>             open           = new ThreadLocal<Boolean>() {

                                                                               @Override
                                                                               protected Boolean initialValue() {
                                                                                   return false;
                                                                               }
                                                                           };

    private static final ThreadLocal<SqlExplainLevel>    sqlExplainLevel = new ThreadLocal<SqlExplainLevel>() {
                                                                                @Override
                                                                                protected SqlExplainLevel initialValue() {
                                                                                    return SqlExplainLevel.EXPPLAN_ATTRIBUTES;
                                                                                }
                                                                            };

    private static final ThreadLocal<PlanOptimizerTracer> OPTIMIZER_TRACER = new ThreadLocal<PlanOptimizerTracer>() {

                                                                               @Override
                                                                               protected PlanOptimizerTracer initialValue() {
                                                                                   return new PlanOptimizerTracer();
                                                                               }
                                                                           };

    public static ThreadLocal<PlanOptimizerTracer> getOptimizerTracer() {
        return OPTIMIZER_TRACER;
    }

    public static boolean isOpen() {
        return open.get();
    }

    public static void setOpen(boolean b) {
        open.set(b);
    }

    public static SqlExplainLevel getSqlExplainLevel() { return sqlExplainLevel.get(); }
    
    public static void setSqlExplainLevel(SqlExplainLevel v) { sqlExplainLevel.set(v); }
}
