package com.alibaba.polardbx.executor.columnar.pruning.predicate;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.columnar.pruning.data.PruneUtils;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruneContext;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * @author fangwu
 */
public class ColumnarPredicatePruningVisitor extends RexVisitorImpl<ColumnPredicatePruningInf> {
    private IndexPruneContext ipc;

    public ColumnarPredicatePruningVisitor(IndexPruneContext ipc) {
        super(false);
        this.ipc = ipc;
    }

    @Override
    public ColumnPredicatePruningInf visitCall(RexCall call) {
        SqlKind kind = call.op.kind;
        switch (kind) {
        case AND:
            return visitAnd(call);
        case OR:
            return visitOr(call);
        case NOT:
            return visitNot(call);
        case EQUALS:
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL:
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
            return visitBinary(call);
        case BETWEEN:
            return visitBetween(call);
        case IN:
            return visitIn(call);
        case IS_NULL:
            return visitIsNull(call);
        case RUNTIME_FILTER:
            return visitRuntimeFilter(call);
        default:
            return null;
        }
    }

    private ColumnPredicatePruningInf visitRuntimeFilter(RexCall call) {
        // todo support min max rf & bloom filter rf
        return null;
    }

    private ColumnPredicatePruningInf visitIsNull(RexCall call) {
        if (!(call.getOperands().get(0) instanceof RexInputRef)) {
            return null;
        }
        RexInputRef field = (RexInputRef) call.getOperands().get(0);
        return new IsNullColumnPredicate(field.getIndex());
    }

    private ColumnPredicatePruningInf visitIn(RexCall call) {
        if (!(call.getOperands().get(0) instanceof RexInputRef)) {
            return null;
        }

        if (!(call.getOperands().get(1) instanceof RexCall && call.getOperands().get(1).isA(SqlKind.ROW))) {
            return null;
        }

        RexInputRef field = (RexInputRef) call.getOperands().get(0);
        RexCall inRexCall = ((RexCall) call.getOperands().get(1));

        if (inRexCall.getOperands().size() > 1) {
            // only support raw string now
            return null;
        }
        if (!(inRexCall.getOperands().get(0) instanceof RexDynamicParam)) {
            return null;
        }
        RexDynamicParam para = (RexDynamicParam) inRexCall.getOperands().get(0);
        SqlTypeName typeName = field.getType().getSqlTypeName();
        return new InColumnPredicate(typeName, field.getIndex(), para.getIndex());
    }

    private ColumnPredicatePruningInf visitBetween(RexCall call) {
        if (!(call.getOperands().get(0) instanceof RexInputRef)) {
            return null;
        }
        if (RexUtil.isConstant(call.getOperands().get(1)) && RexUtil.isConstant(call.getOperands().get(2))) {
            RexInputRef field = (RexInputRef) call.getOperands().get(0);
            Object param1 = PruneUtils.getValueFromRexNode(call.getOperands().get(1), ipc);
            Object param2 = PruneUtils.getValueFromRexNode(call.getOperands().get(2), ipc);
            SqlTypeName typeName = field.getType().getSqlTypeName();
            return new BetweenColumnPredicate(typeName, field.getIndex(), -1, -1, param1, param2);
        }
        return null;
    }

    private ColumnPredicatePruningInf visitBinary(RexCall call) {
        RexInputRef field;
        SqlKind sqlKind = call.getKind();
        if (call.getOperands().get(0) instanceof RexInputRef &&
            RexUtil.isConstant(call.getOperands().get(1))) {
            field = (RexInputRef) call.getOperands().get(0);
            Object param = PruneUtils.getValueFromRexNode(call.getOperands().get(1), ipc);
            SqlTypeName typeName = field.getType().getSqlTypeName();
            return new BinaryColumnPredicate(typeName, field.getIndex(), sqlKind, param);
        } else if (call.getOperands().get(1) instanceof RexInputRef &&
            RexUtil.isConstant(call.getOperands().get(0))) {
            field = (RexInputRef) call.getOperands().get(1);
            Object param = PruneUtils.getValueFromRexNode(call.getOperands().get(0), ipc);
            SqlTypeName typeName = field.getType().getSqlTypeName();
            return new BinaryColumnPredicate(typeName, field.getIndex(), flipSqlKind(sqlKind), param);
        } else {
            return null;
        }

    }

    private SqlKind flipSqlKind(SqlKind sqlKind) {
        switch (sqlKind) {
        case EQUALS:
            return sqlKind;
        case LESS_THAN:
            return SqlKind.GREATER_THAN;
        case LESS_THAN_OR_EQUAL:
            return SqlKind.GREATER_THAN_OR_EQUAL;
        case GREATER_THAN:
            return SqlKind.LESS_THAN;
        case GREATER_THAN_OR_EQUAL:
            return SqlKind.LESS_THAN_OR_EQUAL;
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_BINARY_PREDICATE,
                "not support operator for BinaryColumnPredicate");
        }
    }

    private ColumnPredicatePruningInf visitNot(RexCall call) {
        if (call.getOperands().size() != 1) {
            return null;
        }
        ColumnPredicatePruningInf sub = call.getOperands().get(0).accept(this);
        if (sub != null) {
            return new NotColumnPredicate(sub);
        }
        return null;
    }

    private ColumnPredicatePruningInf visitOr(RexCall call) {
        OrColumnPredicate or = new OrColumnPredicate();
        for (RexNode operand : call.getOperands()) {
            ColumnPredicatePruningInf sub = operand.accept(this);
            if (sub != null) {
                or.add(sub);
            } else {
                return null;
            }
        }
        return or.flat();
    }

    private ColumnPredicatePruningInf visitAnd(RexCall call) {
        AndColumnPredicate and = new AndColumnPredicate();
        for (RexNode operand : call.getOperands()) {
            ColumnPredicatePruningInf sub = operand.accept(this);
            if (sub != null) {
                and.add(sub);
            }
        }
        return and.flat();
    }
}
