package com.alibaba.polardbx.qatest.ddl.balancer.datagenerator;

import java.sql.SQLException;

public interface DataGenerator {

    Object generateData(String fieldType) throws SQLException;

}
