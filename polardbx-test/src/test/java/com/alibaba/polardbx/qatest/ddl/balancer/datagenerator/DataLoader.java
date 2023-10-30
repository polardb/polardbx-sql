package com.alibaba.polardbx.qatest.ddl.balancer.datagenerator;

import org.apache.commons.collections.CollectionUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class DataLoader {

    private String tableName;

    private Connection connection;

    private List<String> fieldNames = new ArrayList<>();
    private List<String> fieldTypes = new ArrayList<>();
    private List<String> nulls = new ArrayList<>();
    private List<String> keys = new ArrayList<>();
    private List<String> defaults = new ArrayList<>();
    private List<String> extras = new ArrayList<>();

    private boolean autoPrimaryKey = true;

    private DataGenerator dataGenerator;

    //行存
    List<List<Object>> allData = new ArrayList<>();

    public static DataLoader create(Connection connection, String tableName, DataGenerator dataGenerator) {
        DataLoader dataLoader = new DataLoader();

        dataLoader.connection = connection;
        dataLoader.tableName = tableName;
        dataLoader.dataGenerator = dataGenerator;

        dataLoader.descTable();
        return dataLoader;
    }

    public void batchInsert(long count) {
        String sql = batchInsertSql(fieldNames.size(), "?");
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (long c = 0; c < count; c++) {
                List<Object> rowData = new ArrayList<>();
                for (int i = 1; i <= fieldTypes.size(); i++) {
                    String fieldType = fieldTypes.get(i - 1);
                    Object columnValue = generateData(fieldType);
                    preparedStatement.setObject(i, columnValue);
                    rowData.add(columnValue);
                }
                allData.add(rowData);

                preparedStatement.addBatch();
                if (c % 500 == 0) {
                    preparedStatement.executeBatch();
                }
            }
            preparedStatement.executeBatch();
//            connection.commit();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void generateBatchInsertSql(String fullPath, int count) throws SQLException {
        final String sqlTemplate = batchInsertSql(fieldNames.size(), "%s");

        try (FileWriter fileWriter = new FileWriter(fullPath)) {

            for (int c = 0; c < count; c++) {
                List<Object> rowData = new ArrayList<>();
                for (int i = 1; i <= fieldTypes.size(); i++) {
                    String fieldType = fieldTypes.get(i - 1);
                    Object columnValue = generateData(fieldType);
                    rowData.add(columnValue);
                }
                allData.add(rowData);
                final String sqlWritten = String.format(sqlTemplate, rowData.toArray()) + ";\n";
                fileWriter.write(sqlWritten);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int columnSize() {
        return fieldNames.size();
    }

    public int rowSize() {
        return allData.size();
    }

    public Object[] getColumnData(int columnIndex) {
        Object[] result = new Object[allData.size()];
        int i = 0;
        for (List<Object> row : allData) {
            result[i++] = row.get(columnIndex);
        }
        return result;
    }

    /*******************************************************************************************************/

    private synchronized void descTable() {
        if (CollectionUtils.isNotEmpty(fieldNames)) {
            return;
        }
        try (Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("desc " + tableName)) {

            while (resultSet.next()) {
                fieldNames.add(resultSet.getString(1));
                fieldTypes.add(resultSet.getString(2));
                nulls.add(resultSet.getString(3));
                keys.add(resultSet.getString(4));
                defaults.add(resultSet.getString(5));
                extras.add(resultSet.getString(6));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private String batchInsertSql(int columnSize, String placeHolder) {
        String sql = String.format("insert into %s values ", tableName);
        StringJoiner joiner = new StringJoiner(",", "(", ")");
        for (int i = 0; i < columnSize; i++) {
            joiner.add(placeHolder);
        }
        sql += joiner.toString();
        return sql;
    }

    private Object generateData(String fieldType) throws SQLException {
        return dataGenerator.generateData(fieldType);
    }

}
