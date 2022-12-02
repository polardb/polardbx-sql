package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

/**
 * @author ximing.yd
 * @date 2022/1/5 5:03 下午
 */
public class SQLShowPartitionsHeatmapStatement extends MySqlStatementImpl implements SQLShowStatement {

    private SQLName timeRange;

    private SQLExpr type;

    @Override
    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, timeRange);
            acceptChild(visitor, type);
        }
        visitor.endVisit(this);
    }

    public SQLName getTimeRange() {
        return timeRange;
    }

    public void setTimeRange(SQLName timeRange) {
        this.timeRange = timeRange;
    }

    public SQLExpr getType() {
        return type;
    }

    public void setType(SQLExpr type) {
        this.type = type;
    }
}
