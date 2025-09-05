package com.alibaba.polardbx.optimizer.partition.pruning;

import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import lombok.Data;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
@Data
public class BuildPartClauseInfoParams {

    protected Integer id;

    /**
     * the operator of part predicate
     */
    protected SqlOperator op;

    /**
     * the kind of part predicate
     */
    protected SqlKind opKind;

    /**
     * The rel row type of plan which is referred by constExpr and input
     */
    protected RelDataType planRelRowType;

    /**
     * the input of part predicate
     */
    protected RexNode input;

    /**
     * the const val of part predicate
     */
    protected RexNode constExpr;
    /**
     * a id to label constExpr uniquely to avoiding computing the constExpr repeatedly
     */
    protected Integer constExprId;

    /**
     * the original predicate of curr PartClauseInfo
     */
    protected RexNode originalPredicate;

    /**
     * The flag that label if the constExpr is null value
     */
    protected boolean isNull = false;

    /**
     * The index of const expr in tuple, used by insert
     */
    protected int indexInTuple;

    /**
     * the part level that the col of input belong to
     */
    protected PartKeyLevel partKeyLevel;

    /**
     * the part strategy that the col of input belong to
     */
    protected PartitionStrategy strategy;

    /**
     * the part key index that the col of input in part key, the index is start from 0.
     */
    protected int partKeyIndex;

    /**
     * the field def of the part key
     */
    protected RelDataType partKeyDataType;

    /**
     * label if const expr is a dynamic expression only, such as pk=?
     */
    protected boolean isDynamicConstOnly = false;

    protected boolean isSubQueryInExpr = false;

    /**
     * Label if const expr is a virtual expr that col= anyValues
     */
    protected boolean isAnyValueEqCond = false;

    /**
     * =========== Begin: params for building SubQueryInPartClauseInfo ==============
     */
    boolean isBuildForSubQueryInPartClauseInfo = false;
    protected List<RexDynamicParam> partColDynamicParams = new ArrayList<>();
    /**
     * The n-th of eqExprClauseItems save the condition of  pn=tmpDynamicParams_n
     */
    protected List<PartClauseItem> eqExprClauseItems = new ArrayList<>();
    protected RexDynamicParam subQueryDynamicParam;

    /**
     * =========== End: params for building SubQueryInPartClauseInfo ==============
     */
    public BuildPartClauseInfoParams() {
    }
}
