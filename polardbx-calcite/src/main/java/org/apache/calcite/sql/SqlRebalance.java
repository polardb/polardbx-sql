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

package org.apache.calcite.sql;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.commons.lang3.BooleanUtils;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Rebalance command for cluster rebalance
 *
 * @author moyi
 * @since 2021/04
 */
@Getter
@Setter
public class SqlRebalance extends SqlDdl {

    private static final SqlSpecialOperator OPERATOR = new SqlRebalanceOperator();

    public enum RebalanceTarget {
        TABLE,
        DATABASE,
        CLUSTER
    }

    /**
     * Options
     */
    public final static String OPTION_POLICY = "POLICY";
    public final static String OPTION_EXPLAIN = "EXPLAIN";
    public final static String OPTION_MAX_ACTIONS = "MAX_ACTIONS";
    public final static String OPTION_MAX_SIZE = "MAX_SIZE";
    public final static String OPTION_DRAIN_NODE = "DRAIN_NODE";
    public final static String OPTION_ASYNC = "async";
    public final static String OPTION_DEBUG = "debug";
    public final static String OPTION_DISK_INFO = "disk_info";

    /**
     * Policies
     */
    public static final String POLICY_SPLIT_PARTITION = "split_partition";
    public static final String POLICY_MERGE_PARTITION = "merge_partition";
    public static final String POLICY_DRAIN_NODE = "drain_node";
    public static final String POLICY_BALANCE_GROUP = "balance_group";
    public static final String POLICY_DATA_BALANCE = "data_balance";
    public static final List<String> ALL_POLICIES = Arrays.asList(
        POLICY_SPLIT_PARTITION,
        POLICY_MERGE_PARTITION,
        POLICY_DRAIN_NODE,
        POLICY_BALANCE_GROUP,
        POLICY_DATA_BALANCE
    );

    /**
     * Max steps of this action
     */
    private int maxActions;
    private int maxPartitionSize;

    /**
     * Option values
     */
    private RebalanceTarget target;
    private String policy;
    private SqlNode tableName;
    private boolean explain = false;
    private boolean async = true;
    private boolean debug = false;
    private String diskInfo;
    private String drainNode;

    public SqlRebalance(SqlParserPos pos, SqlNode tableName) {
        super(OPERATOR, pos);
        this.target = RebalanceTarget.TABLE;
        this.tableName = tableName;
    }

    public SqlRebalance(SqlParserPos pos) {
        super(OPERATOR, pos);
        this.target = RebalanceTarget.CLUSTER;
    }

    public void addOption(String name, SqlNode value) {
        if (name.equalsIgnoreCase(OPTION_MAX_ACTIONS)) {
            this.maxActions = ((SqlLiteral) value).intValue(false);
        } else if (name.equalsIgnoreCase(OPTION_POLICY)) {
            String policy = ((SqlCharStringLiteral) value).getNlsString().getValue();
            if (TStringUtil.isNotBlank(this.policy)) {
                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                    String.format("policy conflicted: %s with %s", policy, this.policy));
            }
            this.policy = policy;
        } else if (name.equalsIgnoreCase(OPTION_MAX_SIZE)) {
            this.maxPartitionSize = ((SqlLiteral) value).intValue(false);
        } else if (name.equalsIgnoreCase(OPTION_EXPLAIN)) {
            this.explain = ((SqlLiteral) value).booleanValue();
        } else if (OPTION_ASYNC.equalsIgnoreCase(name)) {
            this.async = ((SqlLiteral) value).booleanValue();
        } else if (OPTION_DEBUG.equalsIgnoreCase(name)) {
            this.debug = ((SqlLiteral) value).booleanValue();
        } else if (OPTION_DISK_INFO.equalsIgnoreCase(name)) {
            this.diskInfo = ((SqlCharStringLiteral) value).getNlsString().getValue();
        } else if (name.equalsIgnoreCase(OPTION_DRAIN_NODE)) {
            this.drainNode = ((SqlCharStringLiteral) value).getNlsString().getValue();
            this.policy = POLICY_DRAIN_NODE;
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, name + " not supported");
        }
    }

    public void setRebalanceTable(SqlNode tableName) {
        this.target = RebalanceTarget.TABLE;
        this.tableName = tableName;
    }

    public void setRebalanceDatabase() {
        this.target = RebalanceTarget.DATABASE;
    }

    public void setRebalanceCluster() {
        this.target = RebalanceTarget.CLUSTER;
    }

    public boolean isRebalanceCluster() {
        return this.target.equals(RebalanceTarget.CLUSTER);
    }

    public boolean isRebalanceTable() {
        return this.target.equals(RebalanceTarget.TABLE);
    }

    public boolean isRebalanceDatabase() {
        return this.target.equals(RebalanceTarget.DATABASE);
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        // do nothing
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("REBALANCE");
        if (this.target.equals(RebalanceTarget.TABLE)) {
            writer.keyword("TABLE");
            this.tableName.unparse(writer, leftPrec, rightPrec);
        } else if (this.target.equals(RebalanceTarget.DATABASE)) {
            writer.keyword("DATABASE");
        } else if (this.target.equals(RebalanceTarget.CLUSTER)) {
            writer.keyword("CLUSTER");
        }

        if (this.maxActions != 0) {
            writer.keyword("MAX_ACTIONS = ");
            writer.print(String.valueOf(this.maxActions));
        }

        if (TStringUtil.isNotBlank(this.drainNode)) {
            writer.print(" drain_node=" + TStringUtil.quoteString(this.drainNode));
        }
        if (TStringUtil.isNotBlank(this.policy)) {
            writer.print(" POLICY=" + TStringUtil.quoteString(this.policy));
        }
        if (TStringUtil.isNotBlank(this.diskInfo)) {
            writer.print(" DISK_INFO=" + TStringUtil.quoteString(this.diskInfo));
        }

        writer.print(" EXPLAIN=" + BooleanUtils.toStringTrueFalse(this.explain));
        writer.print(" ASYNC=" + BooleanUtils.toStringTrueFalse(this.async));
        writer.print(" DEBUG=" + BooleanUtils.toStringTrueFalse(this.debug));
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(name);
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.REBALANCE;
    }

    public static class SqlRebalanceOperator extends SqlSpecialOperator {

        public SqlRebalanceOperator() {
            super("SQL_REBALANCE", SqlKind.REBALANCE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("RESULT", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));

            return typeFactory.createStructType(columns);
        }
    }
}


