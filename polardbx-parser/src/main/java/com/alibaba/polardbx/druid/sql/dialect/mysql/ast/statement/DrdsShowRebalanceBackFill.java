package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

/**
 * @version 1.0
 * @ClassName DrdsShowRebalanceBackFill
 * @description
 * @Author guxu
 */
public class DrdsShowRebalanceBackFill extends DrdsGenericDDLJob {

    @Override
    public void accept0(MySqlASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

}
