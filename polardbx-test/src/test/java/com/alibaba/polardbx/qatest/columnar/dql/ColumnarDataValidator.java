package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.qatest.validator.DataValidator;

import java.sql.Connection;
import java.util.List;

public class ColumnarDataValidator {
    public static List<List<Object>> selectContentSameAssertWithDiffSql(String tddlSql, String mysqlSql,
                                                                        List<Object> param,
                                                                        Connection mysqlConnection,
                                                                        Connection tddlConnection,
                                                                        boolean allowEmptyResultSet,
                                                                        boolean ignoreException,
                                                                        boolean ignoreMySQLWarnings) {
        ColumnarUtils.waitColumnarOffset(tddlConnection);
        return DataValidator.selectContentSameAssertWithDiffSql(tddlSql, mysqlSql, param, mysqlConnection,
            tddlConnection, allowEmptyResultSet, ignoreException, ignoreMySQLWarnings);
    }
}

