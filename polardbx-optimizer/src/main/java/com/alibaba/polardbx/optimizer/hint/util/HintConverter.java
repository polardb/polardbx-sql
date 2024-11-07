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

package com.alibaba.polardbx.optimizer.hint.util;

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.hint.operator.HintAddOperator;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdCoronaDbJson;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdDynamicBroadcastJoin;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdExtraOperator;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdIndex;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdJoin;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdMasterSlave;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdMergeUnionSize;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdNode;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdPlan;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdQueryBlockName;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdRandomNode;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdScan;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdSocketTimeout;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdSqlDelayCutoff;
import com.alibaba.polardbx.optimizer.hint.operator.HintInventoryOperator;
import com.alibaba.polardbx.optimizer.hint.operator.HintPushAggOperator;
import com.alibaba.polardbx.optimizer.hint.operator.HintPushFilterOperator;
import com.alibaba.polardbx.optimizer.hint.operator.HintPushFromOperator;
import com.alibaba.polardbx.optimizer.hint.operator.HintPushLimitOperator;
import com.alibaba.polardbx.optimizer.hint.operator.HintPushOperator;
import com.alibaba.polardbx.optimizer.hint.operator.HintPushProjectOperator;
import com.alibaba.polardbx.optimizer.hint.operator.HintPushSortOperator;
import com.alibaba.polardbx.optimizer.hint.operator.HintPushdownOperator;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;

import java.util.LinkedList;
import java.util.List;

/**
 * @author chenmo.cm
 */
public class HintConverter {

    public static HintCollection convertCmd(
        SqlNodeList hints, List<HintCmdOperator> cmdHints, boolean testMode, ExecutionContext ec) {
        return convert(hints, null, null, null, cmdHints, testMode, ec);
    }

    public static HintCollection convertPushdown(SqlNodeList hints, List<HintPushdownOperator> pushdownHints,
                                                 ExecutionContext ec) {
        return convert(hints, null, null, pushdownHints, null, false, ec);
    }

    public static HintCollection convertPush(SqlNodeList hints, List<HintPushOperator> pushHints, ExecutionContext ec) {
        return convert(hints, pushHints, null, null, null, false, ec);
    }

    public static class HintCollection {
        private int pushCount = 0;
        private int addCount = 0;
        private int pushdownCount = 0;
        private int constructCount = 0;

        private int cmdCount = 0;
        private int nodeCount = 0;
        private int scanCount = 0;
        private int jsonCount = 0;

        private int directCount = 0;

        private boolean directWithRealTableName = false;

        private boolean hasPlanHint = false;

        public int routeCount = 0;

        public final List<HintPushOperator> pushHintResult = new LinkedList<>();
        public final List<HintAddOperator> addHintResult = new LinkedList<>();
        public final List<HintPushdownOperator> pushdownHintResult = new LinkedList<>();
        public final List<HintCmdOperator> cmdHintResult = new LinkedList<>();
        public HintInventoryOperator inventoryHint = null;

        public final List<String> errorMessages = new LinkedList<>();

        public boolean cmdOnly() {
            return cmdCount > 0 && (pushCount + addCount + pushdownCount + constructCount) <= 0;
        }

        public boolean route() {
            return nodeCount + scanCount + routeCount > 0;
        }

        public boolean routeWithoutScan() {
            return nodeCount + routeCount > 0 && scanCount == 0;
        }

        public boolean usePostPlanner() {
            return (cmdOnly() && !route()) || (inventoryHint != null && inventoryHint.getHints().size() > 0);
        }

        public boolean pushOnly() {
            return pushCount > 0 && (addCount + nodeCount + scanCount + jsonCount + constructCount) <= 0;
        }

        public boolean pushdownOnly() {
            return pushdownCount > 0
                && (addCount + pushCount + nodeCount + scanCount + jsonCount + constructCount) <= 0;
        }

        public boolean directOnly() {
            return directCount > 0 && (directCount == jsonCount)
                && (addCount + pushCount + constructCount + pushdownCount + nodeCount + scanCount) <= 0;
        }

        public boolean nodeOnly() {
            return nodeCount > 0
                && (addCount + pushCount + constructCount + pushdownCount + scanCount + jsonCount) <= 0;
        }

        public boolean pushdownOriginSql() {
            return directOnly() || nodeOnly();
        }

        public boolean pushdownSqlOrRoute() {
            return directOnly() || nodeOnly() || route();
        }

        public boolean pushdownSqlOrRouteWithoutScan() {
            return directOnly() || nodeOnly() || routeWithoutScan();
        }

        public boolean hasPlanHint() {
            return hasPlanHint;
        }

        public void pushHintOperator(HintPushOperator pushOperator) {
            pushHintResult.add(pushOperator);
            pushCount++;
        }

        public void addHintOperator(HintAddOperator addOperator) {
            addHintResult.add(addOperator);
            addCount++;
        }

        public boolean directWithRealTableName() {
            return directWithRealTableName;
        }

        public void pushdownHintOperator(HintPushdownOperator pushdownOperator) {
            pushdownHintResult.add(pushdownOperator);
            pushdownCount++;
        }

        public void cmdHintOperator(HintCmdOperator cmdOperator) {
            cmdHintResult.add(cmdOperator);
            cmdCount++;
            switch (cmdOperator.getType()) {
            case CMD_NODE:
            case CMD_RANDOM_NODE:
                nodeCount++;
                break;
            case CMD_SCAN:
                scanCount++;
                break;
            case CMD_CORONADB_JSON:
                jsonCount++;
                if (((HintCmdCoronaDbJson) cmdOperator).isDirect()) {
                    directCount++;
                } else if (((HintCmdCoronaDbJson) cmdOperator).isUseRouteCondition()) {
                    routeCount++;
                }
                if (((HintCmdCoronaDbJson) cmdOperator).isDirectWithRealTableName()) {
                    directWithRealTableName = true;
                }
                break;
            case CMD_PLAN:
                hasPlanHint = true;
            default:
            } // end of switch
        }

        public void merge(HintCollection other) {
            this.pushCount += other.pushCount;
            this.addCount += other.addCount;
            this.pushdownCount += other.pushdownCount;
            this.constructCount += other.constructCount;
            this.cmdCount += other.cmdCount;
            this.scanCount += other.scanCount;
            this.nodeCount += other.nodeCount;
            this.jsonCount += other.jsonCount;
            this.directCount += other.directCount;
            this.routeCount += other.routeCount;
            this.directWithRealTableName = directWithRealTableName || other.directWithRealTableName;
            this.hasPlanHint = hasPlanHint || other.hasPlanHint;

            this.pushHintResult.addAll(other.pushHintResult);
            this.addHintResult.addAll(other.addHintResult);
            this.pushdownHintResult.addAll(other.pushdownHintResult);
            this.cmdHintResult.addAll(other.cmdHintResult);
            this.errorMessages.addAll(other.errorMessages);
            if (null == this.inventoryHint || null == other.inventoryHint) {
                this.inventoryHint = (null == this.inventoryHint) ? other.inventoryHint : this.inventoryHint;
            } else {
                this.inventoryHint.getHints().putAll(other.inventoryHint.getHints());
            }
        }
    }

    public static HintCollection convert(SqlNodeList hints, List<HintPushOperator> pushHints,
                                         List<HintAddOperator> addHints, List<HintPushdownOperator> pushdownHints,
                                         List<HintCmdOperator> cmdHints,
                                         boolean testMode, ExecutionContext ec) {
        final HintCollection hintCollection = new HintCollection();

        for (SqlNode hint : hints) {
            if (hint instanceof SqlBasicCall) {
                final SqlBasicCall hintOp = (SqlBasicCall) hint;
                final HintType hintType = HintType.of(hintOp.getOperator().getName());

                if (null == pushHints && hintType.isA(HintType.PUSH_HINT)) {
                    hintCollection.pushCount++;
                    continue;
                }

                if (null == addHints && hintType.isA(HintType.ADD_HINT)) {
                    hintCollection.addCount++;
                    continue;
                }

                if (null == pushdownHints && hintType.isA(HintType.PUSHDOWN_HINT)) {
                    hintCollection.pushdownCount++;
                    continue;
                }

                if (null == cmdHints && hintType.isA(HintType.CMD_HINT)) {
                    hintCollection.cmdCount++;
                    switch (hintType) {
                    case CMD_NODE:
                        hintCollection.nodeCount++;
                        break;
                    case CMD_RANDOM_NODE:
                        hintCollection.nodeCount++;
                        break;
                    case CMD_SCAN:
                        hintCollection.scanCount++;
                        break;
                    case CMD_CORONADB_JSON:
                        hintCollection.jsonCount++;
                        break;
                    default:
                    } // end of switch
                    continue;
                }

                switch (hintType) {
                case PUSHDOWN:
                    hintCollection.pushdownHintOperator(new HintPushdownOperator(hintOp, ec));
                    break;
                case CONSTRUCT:
                    hintCollection.constructCount++;
                    break;
                case PUSH_SORT:
                    hintCollection.pushHintOperator(new HintPushSortOperator(hintOp, testMode, ec));
                    break;
                case PUSH_PJ:
                    hintCollection.pushHintOperator(new HintPushProjectOperator(hintOp, testMode, ec));
                    break;
                case PUSH_AGG:
                    hintCollection.pushHintOperator(new HintPushAggOperator(hintOp, testMode, ec));
                    break;
                case PUSH_FT:
                    hintCollection.pushHintOperator(new HintPushFilterOperator(hintOp, testMode, ec));
                    break;
                case PUSH_FROM:
                    hintCollection.pushHintOperator(new HintPushFromOperator(hintOp, testMode, ec));
                    break;
                case PUSH_LMT:
                    hintCollection.pushHintOperator(new HintPushLimitOperator(hintOp, testMode, ec));
                    break;
                case ADD_MS:
                    hintCollection.addCount++;
                    break;
                case ADD_TS:
                    hintCollection.addCount++;
                    break;
                case ADD_PJ:
                    hintCollection.addCount++;
                    break;
                case ADD_AGG:
                    hintCollection.addCount++;
                    break;
                case ADD_FT:
                    hintCollection.addCount++;
                    break;
                case ADD_LMT:
                    hintCollection.addCount++;
                    break;
                case ADD_UN:
                    hintCollection.addCount++;
                    break;

                case CMD_MASTER:
                case CMD_SLAVE:
                case CMD_FOLLOWER:
                    hintCollection.cmdHintOperator(new HintCmdMasterSlave(hintOp, ec));
                    break;
                case CMD_SOCKET_TIMEOUT:
                    hintCollection.cmdHintOperator(new HintCmdSocketTimeout(hintOp, ec));
                    break;
                case CMD_DYNAMIC_BROADCAST_JOIN:
                    hintCollection.cmdHintOperator(new HintCmdDynamicBroadcastJoin(hintOp, ec));
                    break;
                case CMD_BKA_JOIN:
                case CMD_NL_JOIN:
                case CMD_SORT_MERGE_JOIN:
                case CMD_HASH_JOIN:
                case CMD_MATERIALIZED_SEMI_JOIN:
                case CMD_SEMI_HASH_JOIN:
                case CMD_SEMI_NL_JOIN:
                case CMD_ANTI_NL_JOIN:
                case CMD_ANTI_HASH_JOIN:
                case CMD_SEMI_SORT_MERGE_JOIN:
                case CMD_NO_JOIN:
                case CMD_SEMI_BKA_JOIN:
                case CMD_HASH_OUTER_JOIN:
                case CMD_HASH_GROUP_JOIN:
                    hintCollection.cmdHintOperator(new HintCmdJoin(hintOp, ec));
                    break;
                case CMD_SQL_DELAY_CUTOFF:
                    hintCollection.cmdHintOperator(new HintCmdSqlDelayCutoff(hintOp, ec));
                    break;
                case CMD_EXTRA:
                    hintCollection.cmdHintOperator(new HintCmdExtraOperator(hintOp, ec));
                    break;
                case CMD_NODE:
                    if (ConfigDataMode.isFastMock()) {
                        break;
                    }
                    hintCollection.cmdHintOperator(new HintCmdNode(hintOp, ec));
                    break;
                case CMD_RANDOM_NODE:
                    if (ConfigDataMode.isFastMock()) {
                        break;
                    }
                    hintCollection.cmdHintOperator(new HintCmdRandomNode(hintOp, ec));
                    break;
                case CMD_SCAN:
                    hintCollection.cmdHintOperator(new HintCmdScan(hintOp, ec));
                    break;
                case CMD_CORONADB_JSON:
                    hintCollection.cmdHintOperator(new HintCmdCoronaDbJson(hintOp, ec));
                    break;
                case CMD_QUERY_BLOCK_NAME:
                    hintCollection.cmdHintOperator(new HintCmdQueryBlockName(hintOp, ec));
                    break;
                case CMD_MERGE_UNION_SIZE:
                    hintCollection.cmdHintOperator(new HintCmdMergeUnionSize(hintOp, ec));
                    break;
                case CMD_INDEX:
                    hintCollection.cmdHintOperator(new HintCmdIndex(hintOp, ec));
                    break;
                case CMD_PLAN:
                    hintCollection.cmdHintOperator(new HintCmdPlan(hintOp, ec));
                    break;
                case INVENTORY_COMMIT_ON_SUCCESS:
                case INVENTORY_ROLLBACK_ON_FAIL:
                    if (null == hintCollection.inventoryHint) {
                        hintCollection.inventoryHint = new HintInventoryOperator(hintOp, ec);
                    }
                    if (hintOp.operands.length > 0) {
                        hintCollection.errorMessages.add(
                            "Inventory hint(commit_on_success or rollback_on_fail) not support parameters, you should use commit_on_success() or rollback_on_fail()");
                    } else {
                        hintCollection.inventoryHint.getHints().put(hintType, 0);
                    }
                    break;
                case INVENTORY_TARGET_AFFECT_ROW:
                    if (hintCollection.inventoryHint == null) {
                        hintCollection.inventoryHint = new HintInventoryOperator(hintOp, ec);
                    }
                    String errorMessage = null;
                    if (hintOp.operands.length != 1 || !(hintOp.operands[0] instanceof SqlNumericLiteral)) {
                        errorMessage =
                            "Inventory hint(target_affect_row) should have one parameter, and it's type should be integer, e.g. target_affect_row(1)";
                    } else {
                        try {
                            Integer value = Integer.valueOf(((SqlNumericLiteral) hintOp.operands[0]).toValue());
                            hintCollection.inventoryHint.getHints().put(hintType, value);
                        } catch (NumberFormatException e) {
                            errorMessage =
                                "Parameter with target_affect_row should be integer, e.g. you should use target_affect_row(1)";
                        }
                    }
                    if (null != errorMessage) {
                        hintCollection.errorMessages.add(errorMessage);
                    }
                    break;
                case NONE:
                default:
                    hintCollection.errorMessages.add("Illegal hint: " + hint.toString());
                    break;
                } // end of switch
            } // end of if
        } // end of for

        if (null != pushHints && hintCollection.pushHintResult.size() > 0) {
            pushHints.addAll(hintCollection.pushHintResult);
        }

        if (null != addHints && hintCollection.addHintResult.size() > 0) {
            addHints.addAll(hintCollection.addHintResult);
        }

        if (null != pushdownHints && hintCollection.pushdownHintResult.size() > 0) {
            pushdownHints.addAll(hintCollection.pushdownHintResult);
        }

        if (null != cmdHints && hintCollection.cmdHintResult.size() > 0) {
            cmdHints.addAll(hintCollection.cmdHintResult);
        }

        return hintCollection;
    }
}
