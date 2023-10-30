package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.google.common.collect.Lists;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;

public abstract class BaseLoadDataTest extends BaseTestCase {
    protected static String PATH = Thread.currentThread().getContextClassLoader().getResource(".").getPath();

    protected Connection mysqlConnection;
    protected Connection tddlConnection;
    protected String baseOneTableName;

    protected void writeToFile(String content, String filePath) {
        try {
            FileWriter fw = new FileWriter(filePath);
            fw.write(content);
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void dropTable(String tableName) {
        String sql = "drop table if exists  " + tableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    protected void deleteFile(String filePath) {
        File file = new File(filePath);
        if (file.exists()) {
            file.delete();
        }
    }

    public static List<Object[]> cartesianProduct(Object[]... arrays) {
        List[] lists = Arrays.stream(arrays)
            .map(Arrays::asList)
            .toArray(List[]::new);
        List<List<Object>> result = Lists.cartesianProduct(lists);
        return result.stream()
            .map(List::toArray)
            .collect(Collectors.toList());
    }
}
