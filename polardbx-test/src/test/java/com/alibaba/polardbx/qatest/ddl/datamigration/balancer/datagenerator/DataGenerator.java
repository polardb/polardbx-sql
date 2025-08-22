package com.alibaba.polardbx.qatest.ddl.datamigration.balancer.datagenerator;

import java.sql.SQLException;

public interface DataGenerator {

    Object generateData(String fieldType) throws SQLException;

}
