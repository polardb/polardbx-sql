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

package com.alibaba.polardbx.common.constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class CpuStatAttribute {


    public static final String EXTRACT_HINT = "HintExtraction";
    public static final String PARAMETERIZE_SQL = "SqlParameterization";

    public static final String PARSE_SQL = "SqlParsing";

    public static final String OPTIMIZE_PLAN = "PlanOptimization";

    public static final String BUILD_DISTRIBUTED_PLAN = "DistributedPlanBuilding";

    public static final String BUILD_PLAN_EXECUTOR = "PlanExecutorBuilding";

    public static final String CLOSE_AND_LOG = "CloseAndLog";

    public static final String CREATE_CONN_TC_SUM = "(Phy)CreateConnTimeCostSum";
    public static final String CREATE_CONN_TC_MAX = "(Phy)CreateConnTimeCostMax";

    public static final String WAIT_CONN_TC_SUM = "(Phy)WaitGetConnTimeCostSum";
    public static final String WAIT_CONN_TC_MAX = "(Phy)WaitGetConnTimeCostMax";

    public static final String INIT_CONN_TC_SUM = "(Phy)InitConnTimeCostSum";
    public static final String INIT_CONN_TC_MAX = "(Phy)InitConnTimeCostMax";

    public static final String PREPARE_STMT_TS_SUM = "(Phy)PrepareStmtTimeCostSum";
    public static final String PREPARE_STMT_TS_MAX = "(Phy)PrepareStmtTimeCostMax";

    public static final String PHY_SQL_EXEC_TS_SUM = "(Phy)SqlExecTimeCostSum";
    public static final String PHY_SQL_EXEC_TS_MAX = "(Phy)SqlExecTimeCostMax";

    public static final String PHY_SQL_FETCH_RS_TS_SUM = "(Phy)SqlFetchRsTimeCostSum";
    public static final String PHY_SQL_FETCH_RS_TS_MAX = "(Phy)SqlFetchRsTimeCostMax";

    public static final String PHY_SQL_CLOSE_RS_TS_SUM = "(Phy)SqlCloseRsTimeCostSum";

    public static final String PLAN_EXECUTION = "PlanExecution";

    public static final String TOTAL = "Total";
    public static final String ROOT = "Root";
    public static final String LOGICAL_TIME_COST = "LogicalTimeCost";
    public static final String PHYSICAL_TIME_COST = "PhysicalTimeCost";
    public static final String TOTAL_TIME_COST = "TotalTimeCost";

    protected static List<CpuStatAttr> childrenAttrListOfRoot = new ArrayList<>();
    protected static Map<CpuStatAttr, List<CpuStatAttr>> attributeRelations =
        new HashMap<CpuStatAttr, List<CpuStatAttr>>();

    protected static void registerAttribute(CpuStatAttr attr, CpuStatAttr parentAttr) {
        List<CpuStatAttr> childrenAttrList = attributeRelations.get(parentAttr);
        if (childrenAttrList == null) {
            childrenAttrList = new ArrayList<>();
            attributeRelations.put(parentAttr, childrenAttrList);
        }
        childrenAttrList.add(attr);
        if (parentAttr.equals(ROOT)) {
            childrenAttrListOfRoot.add(attr);
        }
    }

    public enum CpuStatAttr {
        EXTRACT_HINT(CpuStatAttribute.EXTRACT_HINT),
        PARAMETERIZE_SQL(CpuStatAttribute.PARAMETERIZE_SQL), PARSE_SQL(CpuStatAttribute.PARSE_SQL),
        OPTIMIZE_PLAN(CpuStatAttribute.OPTIMIZE_PLAN), BUILD_DISTRIBUTED_PLAN(CpuStatAttribute.BUILD_DISTRIBUTED_PLAN),
        BUILD_PLAN_EXECUTOR(CpuStatAttribute.BUILD_PLAN_EXECUTOR), CLOSE_AND_LOG(CpuStatAttribute.CLOSE_AND_LOG),
        PLAN_EXECUTION(CpuStatAttribute.PLAN_EXECUTION), ROOT(CpuStatAttribute.ROOT);

        final int attrId;
        final String attrName;

        CpuStatAttr(String attrName) {
            this.attrName = attrName;
            this.attrId = this.ordinal();
        }

        public int getAttrId() {
            return attrId;
        }

        public String getAttrName() {
            return attrName;
        }
    }

    static {
        registerAttribute(CpuStatAttr.EXTRACT_HINT, CpuStatAttr.ROOT);
        registerAttribute(CpuStatAttr.PARAMETERIZE_SQL, CpuStatAttr.ROOT);
        registerAttribute(CpuStatAttr.PARSE_SQL, CpuStatAttr.ROOT);
        registerAttribute(CpuStatAttr.OPTIMIZE_PLAN, CpuStatAttr.ROOT);
        registerAttribute(CpuStatAttr.BUILD_DISTRIBUTED_PLAN, CpuStatAttr.ROOT);
        registerAttribute(CpuStatAttr.CLOSE_AND_LOG, CpuStatAttr.ROOT);
    }

    public static List<CpuStatAttr> getChildrenAttr(CpuStatAttr attr) {
        if (attr.equals(ROOT)) {
            return childrenAttrListOfRoot;
        } else {
            List<CpuStatAttr> childrenAttrList = attributeRelations.get(attr);
            return childrenAttrList;
        }
    }

}
