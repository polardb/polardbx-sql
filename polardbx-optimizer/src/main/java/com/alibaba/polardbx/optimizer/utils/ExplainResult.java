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

package com.alibaba.polardbx.optimizer.utils;

import java.util.EnumSet;

public class ExplainResult {

    public static final String USE_LAST_DATA_NODE = "_USE_LAST_DATA_NODE_";
    public static final String LAST_SEQUENCE_OP = "LAST_SEQUENCE_OP";
    public static final String LAST_SEQUENCE_VAL = "LAST_SEQUENCE_VAL";
    public static final String SEQUENCE_INCREMENT = "SEQUENCE_INCREMENT";
    public static final String SEQUENCE_TYPE = "SEQUENCE_TYPE";
    public static final String DUAL_GROUP = "DUAL_GROUP";
    public static final String SHADOW_DB = "SHADOW_DB";
    public static final String INFORMATION_SCHEMA = "INFORMATION_SCHEMA";
    public static final String PERFORMANCE_SCHEMA = "PERFORMANCE_SCHEMA";
    public static final String QUERY_SHADOW_DB = "QUERY_SHADOW_DB";
    public static final String QUERY_INFORMATION_SCHEMA = "QUERY_INFORMATION_SCHEMA";
    public static final String QUERY_PERFORMANCE_SCHEMA = "QUERY_PERFORMANCE_SCHEMA";
    public static final EnumSet<ExplainMode> EXPLAIN_SHARDING = EnumSet.of(ExplainMode.SHARDING);

    public int explainBeginIndex;
    public int explainIndex;
    public ExplainMode explainMode;

    public static enum ExplainMode {

        // Show logicalView Plan
        LOGICALVIEW,
        /**
         * 逻辑模式,不输出物理表
         */
        LOGIC,
        /**
         * 简略模式,输出物理表,会对LOGIC相同的做合并
         */
        SIMPLE,
        /**
         * 详细模式，在simple模式下，输出列信息
         */
        DETAIL,
        /**
         * 物理执行模式,显示物理执行的执行计划信息
         */
        EXECUTE,
        /**
         * 显示Local/MPP模式的物理Fragment信息
         */
        PHYSICAL,
        /**
         * 优化器详细模式，显示优化过程
         */
        OPTIMIZER,
        /**
         * 显示每张逻辑表需要扫描哪些分片
         */
        SHARDING,
        // Show costs estimated by cost-based optimizer
        COST,
        // Show actual execution cost
        ANALYZE,
        // Show baselineInfo id and planInfoId of this plan
        BASELINE,
        // Show plan info with json format
        JSON_PLAN,
        // index and other advisor
        ADVISOR,
        // the statistics needed by the sql
        STATISTICS,
        // show vectorized plan
        VEC,
        // show statistic detail
        COST_TRACE,
        // show pipeline level stats
        PIPELINE,
        // show columnar snapshot
        SNAPSHOT;

        public boolean isLogic() {
            return this == LOGIC || isSimple();
        }

        public boolean isSimple() {
            return this == SIMPLE;
        }

        public boolean isDetail() {
            return this == DETAIL || isExecute();
        }

        public boolean isExecute() {
            return this == EXECUTE;
        }

        public boolean isPhysical() {
            return this == PHYSICAL;
        }

        public boolean isOptimizer() {
            return this == OPTIMIZER;
        }

        public boolean isSharding() {
            return this.isA(EXPLAIN_SHARDING);
        }

        public boolean isCost() {
            return this == COST || this == COST_TRACE;
        }

        public boolean isCostTrace() {
            return this == COST_TRACE;
        }

        public boolean isAnalyze() {
            return this == ANALYZE;
        }

        public boolean isBaseLine() {
            return this == BASELINE;
        }

        public boolean isJsonPlan() {
            return this == JSON_PLAN;
        }

        public boolean isLogicalView() {
            return this == LOGICALVIEW;
        }

        public boolean isVec() {
            return this == VEC;
        }

        public boolean isAdvisor() {
            return this == ADVISOR;
        }

        public boolean isStatistics() {
            return this == STATISTICS;
        }

        public boolean isPipeline() {
            return this == PIPELINE;
        }

        public boolean isSnapshot() {
            return this == SNAPSHOT;
        }

        public boolean isA(EnumSet enumSet) {
            return null != enumSet && enumSet.contains(this);
        }
    }

    public static boolean isExplainOptimizer(ExplainResult er) {
        return er == null ? false : er.explainMode.isOptimizer();
    }

    public static boolean isExplainSharding(ExplainResult er) {
        return er == null ? false : er.explainMode.isSharding();
    }

    public static boolean isExplainSimple(ExplainResult er) {
        return er == null ? false : er.explainMode.isSimple();
    }

    public static boolean isExplainExecute(ExplainResult er) {
        return er == null ? false : er.explainMode.isExecute();
    }

    public static boolean isPhysicalFragment(ExplainResult er) {
        return er == null ? false : er.explainMode.isPhysical();
    }

    public static boolean isExplainCost(ExplainResult er) {
        return er == null ? false : er.explainMode.isCost();
    }

    public static boolean isExplainCostTrace(ExplainResult er) {
        return er == null ? false : er.explainMode.isCostTrace();
    }

    public static boolean isExplainAnalyze(ExplainResult er) {
        return er == null ? false : er.explainMode.isAnalyze();
    }

    public static boolean isExplainJsonPlan(ExplainResult er) {
        return er == null ? false : er.explainMode.isJsonPlan();
    }

    public static boolean isExplainLogicalView(ExplainResult er) {
        return er == null ? false : er.explainMode.isLogicalView();
    }

    public static boolean isExplainAdvisor(ExplainResult er) {
        return er == null ? false : er.explainMode.isAdvisor();
    }

    public static boolean isExplainStatistics(ExplainResult er) {
        return er != null && er.explainMode.isStatistics();
    }

    public static boolean isExplainPipeline(ExplainResult er) {
        return er != null && er.explainMode.isPipeline();
    }

    public static boolean isExplainSnapshot(ExplainResult er) {
        return er != null && er.explainMode.isSnapshot();
    }

    public static boolean isExplainVec(ExplainResult er) {
        return er == null ? false : er.explainMode.isVec();
    }

    public static boolean isSuitableForDirectMode(ExplainResult er) {
        if (isExplainOptimizer(er) ||
            isExplainAdvisor(er) ||
            isExplainStatistics(er) ||
            isExplainJsonPlan(er) ||
            isExplainExecute(er) ||
            isExplainSharding(er)) {
            return false;
        }
        return true;
    }
}
