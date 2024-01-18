package com.alibaba.polardbx.qatest.ddl.balancer;

import org.junit.Test;

import java.util.List;

public class CreateTableBeans {

    public List<String> getSqlTemplates() {
        return sqlTemplates;
    }

    public void setSqlTemplates(List<String> sqlTemplates) {
        this.sqlTemplates = sqlTemplates;
    }

    public List<String> sqlTemplates;

}