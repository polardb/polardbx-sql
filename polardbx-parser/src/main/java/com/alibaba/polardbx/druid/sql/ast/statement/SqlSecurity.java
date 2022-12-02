package com.alibaba.polardbx.druid.sql.ast.statement;

public enum SqlSecurity {
    // default value
    DEFINER("DEFINER"),
    INVOKER("INVOKER");

    private String content;

    SqlSecurity(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return content;
    }
}
