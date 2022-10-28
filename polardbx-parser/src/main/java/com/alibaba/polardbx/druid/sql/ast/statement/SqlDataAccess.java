package com.alibaba.polardbx.druid.sql.ast.statement;

public enum SqlDataAccess {
    // default value
    CONTAINS_SQL("CONTAINS SQL"),
    NO_SQL("NO SQL"),
    READS_SQL_DATA("READS SQL DATA"),
    MODIFIES_SQL_DATA("MODIFIES SQL DATA");

    private String content;

    SqlDataAccess(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return content;
    }
}
