package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.gms.lbac.LBACSecurityManager;
import com.alibaba.polardbx.gms.lbac.PolarSecurityLabelColumn;
import com.alibaba.polardbx.gms.lbac.LBACSecurityPolicy;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author pangzhaoxing
 */
public class LBACRowAccessChecker extends RelShuttleImpl {

    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());

    private boolean readTable = true;

    private boolean writeTable = false;

    private void setTableRW(boolean read, boolean write) {
        this.readTable = read;
        this.writeTable = write;
    }

    @Override
    public RelNode visit(TableScan tableScan) {
        if (!isSecuredWithLBAC(tableScan.getTable())) {
            return super.visit(tableScan);
        }
        if (!readTable && !writeTable) {
            return super.visit(tableScan);
        }

        int inputIndex = getPSLField(tableScan.getTable()).getIndex();
        RexBuilder rexBuilder = new RexBuilder(TYPE_FACTORY);
        RexInputRef inputRef = rexBuilder.makeInputRef(tableScan, inputIndex);
        RexNode condition;
        if (readTable && writeTable) {
            RexNode readCondition = rexBuilder.makeCall(TddlOperatorTable.LBAC_READ, inputRef);
            RexNode writeCondition = rexBuilder.makeCall(TddlOperatorTable.LBAC_WRITE, inputRef);
            condition = rexBuilder.makeCall(TddlOperatorTable.AND, readCondition, writeCondition);
        } else {
            condition = readTable ? rexBuilder.makeCall(TddlOperatorTable.LBAC_READ, inputRef) :
                rexBuilder.makeCall(TddlOperatorTable.LBAC_WRITE, inputRef);
        }
        LogicalFilter logicalFilter = LogicalFilter.create(super.visit(tableScan), condition);
        return logicalFilter;
    }

    private boolean isSecuredWithLBAC(RelOptTable table) {
        return getPolicy(table) != null && getPSLField(table) != null;
    }

    private LBACSecurityPolicy getPolicy(RelOptTable table) {
        List<String> tableName = table.getQualifiedName();
        return LBACSecurityManager.getInstance().getTablePolicy(tableName.get(0), tableName.get(1));
    }

    private RelDataTypeField getPSLField(RelOptTable table) {
        return table.getRowType().getField(PolarSecurityLabelColumn.COLUMN_NAME, false, false);
    }

    public RelNode visit(TableModify modify) {
        switch (modify.getOperation()) {
        case UPDATE:
        case REPLACE:
            return visitUpdate(modify);
        case INSERT:
            return visitInsert(modify);
        case DELETE:
            return visitDelete(modify);
        default:
            if (!isSecuredWithLBAC(modify.getTable())) {
                return super.visit(modify);
            }
            throw new UnsupportedOperationException();
        }
    }

    /**
     * 如果 UPDATE 语句更新了安全标签列的值，则需要进行 INSERT 类似的权限检查
     * <p>
     * com.alibaba.polardbx.repo.mysql.handler.LogicalModifyHandler#handle
     * LogicalModify的执行逻辑是先执行input RelNode，然后再执行pushdown modify。
     * <p>
     * LogicalTableModify(Update)
     * |
     * LogicalProject/LogicalSort
     * |
     * LogicalTableScan
     */
    public RelNode visitUpdate(TableModify modify) {
        if (!isSecuredWithLBAC(modify.getTable())) {
            return super.visit(modify);
        }
        //让下游的TableScan加上lbac filter
        setTableRW(true, true);
        modify = (TableModify) super.visit(modify);

        //判断update column是否有psl列
        Set<Integer> pslIndexes = new HashSet<>();
        for (int i = 0; i < modify.getUpdateColumnList().size(); i++) {
            if (PolarSecurityLabelColumn.COLUMN_NAME.equalsIgnoreCase(modify.getUpdateColumnList().get(i))) {
                pslIndexes.add(i);
            }
        }
        if (pslIndexes.size() == 0) {
            return modify;
        }

        RexBuilder rexBuilder = new RexBuilder(TYPE_FACTORY);
        RelNode input = modify.getInput();
        //LogicalModify的input节点RowType = 源表的所有列 + update更新的所有列
        int offset = input.getRowType().getFieldList().size() - modify.getUpdateColumnList().size();

        //在update source上面加上LBAC_WRITE_STRICT_CHECK
        List<RexNode> rexNodes = new ArrayList<>();
        for (int i = 0; i < input.getRowType().getFieldList().size(); i++) {
            RexNode rexNode = rexBuilder.makeInputRef(input, i);
            if (pslIndexes.contains(i - offset)) {
                rexNode = rexBuilder.makeCall(TddlOperatorTable.LBAC_WRITE_STRICT_CHECK, rexNode);
            }
            rexNodes.add(rexNode);
        }
        LogicalProject project = LogicalProject.create(input, rexNodes, input.getRowType());

        //LogicalModify的SourceExpressionList和UpdateColumnList()是一一对应的
        List<RexNode> projects = project.getProjects();
        for (int i = 0; i < modify.getUpdateColumnList().size(); i++) {
            modify.getSourceExpressionList().set(i, projects.get(offset + i));
        }

        return modify.copy(modify.getTraitSet(), Collections.singletonList(project));
    }

    /**
     * 如果用户在 INSERT 时不填数据的安全标签，默认填充用户自己的安全标签。
     * 用户在 INSERT 时可以把安全标签填成空“()”，表示所有人都可以访问。
     * 如果 INSERT 语句指定了安全标签的值，则需要进行权限检查。指定的安全标签可以小于（>）用户自己的安全标签。如果指定的安全标签超出（>）用户自身的安全标签，需要有安全管理员权限。
     * <p>
     * polardbx logical insert的执行：
     * com.alibaba.polardbx.repo.mysql.handler.LogicalInsertHandler#handle
     * 判断logicalInsert.isSourceSelect()，由于这里将TableModify的input换成的project，所以logicalInsert.isSourceSelect()一定为true
     * 所以logical insert的执行会先执行selectSource，再执行pushdown insert。这样保证LBAC_USER_WRITE_LABEL和LBAC_WRITE_STRICT_CHECK方法一定可以执行。
     * <p>
     * LogicalTableModify(Insert)
     * |
     * LogicalProject/LogicalSort
     * |
     * LogicalTableScan or LogicalValues
     */
    public RelNode visitInsert(TableModify modify) {
        if (!isSecuredWithLBAC(modify.getTable())) {
            return super.visit(modify);
        }
        //让下游的TableScan加上lbac filter，注意这时writeTable为false,因为下游如果有TableScan，则是insert source
        setTableRW(true, true);
        modify = (TableModify) super.visit(modify);
        RelNode input = modify.getInput();
        RelDataTypeField pslField = input.getRowType().getField(PolarSecurityLabelColumn.COLUMN_NAME, false, false);
        LogicalProject project;
        RexBuilder rexBuilder = new RexBuilder(TYPE_FACTORY);
        if (pslField == null) {
            //如果没有PSL列，则需默认补充user write label
            List<RexNode> rexNodes = new ArrayList<>();
            for (RelDataTypeField field : input.getRowType().getFieldList()) {
                rexNodes.add(rexBuilder.makeInputRef(input, field.getIndex()));
            }
            RexLiteral policy = rexBuilder.makeLiteral(getPolicy(modify.getTable()).getPolicyName());
            rexNodes.add(rexBuilder.makeCall(TddlOperatorTable.LBAC_USER_WRITE_LABEL, policy));
            List<String> filedNames = new ArrayList<>(input.getRowType().getFieldNames());
            filedNames.add(PolarSecurityLabelColumn.COLUMN_NAME);
            project = LogicalProject.create(input, rexNodes, filedNames);
        } else {
            //如果有PSL列, 在insert source上面加上LBAC_WRITE_STRICT_CHECK
            List<RexNode> rexNodes = new ArrayList<>();
            for (RelDataTypeField field : input.getRowType().getFieldList()) {
                RexNode rexNode = rexBuilder.makeInputRef(input, field.getIndex());
                if (field.getIndex() == pslField.getIndex()) {
                    rexNode = rexBuilder.makeCall(TddlOperatorTable.LBAC_WRITE_STRICT_CHECK, rexNode);
                }
                rexNodes.add(rexNode);
            }
            project = LogicalProject.create(input, rexNodes, input.getRowType());
        }
        return modify.copy(modify.getTraitSet(), Collections.singletonList(project));
    }

    /**
     * com.alibaba.polardbx.repo.mysql.handler.LogicalModifyHandler#handle
     * <p>
     * LogicalTableModify(Delete)
     * |
     * LogicalTableScan
     */
    public RelNode visitDelete(TableModify modify) {
        if (!isSecuredWithLBAC(modify.getTable())) {
            return super.visit(modify);
        }
        //让下游的TableScan加上lbac filter
        setTableRW(true, true);
        return super.visit(modify);

    }

    @Override
    public RelNode visit(RelNode other) {
        if (other instanceof TableModify) {
            return visit((TableModify) other);
        } else {
            return super.visit(other);
        }
    }
}
