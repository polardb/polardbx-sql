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
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    public List<Map<String, Object>> rows = new ArrayList<>();

    private boolean autoPrimaryKey = true;

    private DataGenerator dataGenerator;

    //行存
//    List<List<Object>> allData = new ArrayList<>();

    public static DataLoader create(Connection connection, String tableName, DataGenerator dataGenerator) {
        DataLoader dataLoader = new DataLoader();

        dataLoader.connection = connection;
        dataLoader.tableName = tableName;
        dataLoader.dataGenerator = dataGenerator;

        dataLoader.descTable();
        return dataLoader;
    }

    public static DataLoader create(Connection connection, String tableName, List<Map<String, Object>> rows) {
        DataLoader dataLoader = new DataLoader();
        dataLoader.connection = connection;
        dataLoader.tableName = tableName;
        dataLoader.rows = rows;
        dataLoader.descTable();
        return dataLoader;
    }

    public void batchInsert(long count, Boolean fastMode) {
        String sql = batchInsertSql(fieldNames.size(), "?", fastMode);
        int batchSize = fastMode ? 4096 : 512;
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (long c = 0; c < count; c++) {
//                List<Object> rowData = new ArrayList<>();
                for (int i = 1; i <= fieldTypes.size(); i++) {
                    String fieldType = fieldTypes.get(i - 1);
                    Object columnValue = generateData(fieldType);
                    if (extras.get(i - 1).equalsIgnoreCase("auto_increment")) {
                        preparedStatement.setNull(i, Types.INTEGER);
                    } else if (fieldType.startsWith("int") || fieldType.startsWith("bigint")) {
                        preparedStatement.setInt(i, (Integer) columnValue);
                    } else if (fieldType.startsWith("varchar") || fieldType.startsWith("binary")) {
                        preparedStatement.setString(i, (String) columnValue);
                    } else {
                        preparedStatement.setObject(i, columnValue);
                    }
//                    rowData.add(columnValue);
                }
//                allData.add(rowData);

                preparedStatement.addBatch();
                if (c % batchSize == 0) {
                    preparedStatement.executeBatch();
                }
            }
            preparedStatement.executeBatch();
//            connection.commit();

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void batchInsertFromRow(long count, Boolean fastMode) {
        String sql = batchInsertSql(fieldNames.size(), "?", fastMode);
        int batchSize = fastMode ? 4096 : 512;
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (long c = 0; c < count; c++) {
//                List<Object> rowData = new ArrayList<>();
                int rowNum = (int) c;
                for (int i = 1; i <= fieldTypes.size(); i++) {
                    String fieldType = fieldTypes.get(i - 1);
                    String columnName = fieldNames.get(i - 1);
                    Object columnValue = rows.get(rowNum).get(columnName);
                    preparedStatement.setObject(i, columnValue);
//                    rowData.add(columnValue);
                }
//                allData.add(rowData);

                preparedStatement.addBatch();
                if (c % batchSize == 0) {
                    preparedStatement.executeBatch();
                }
            }
            preparedStatement.executeBatch();
//            connection.commit();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
//
//    public void generateBatchInsertSql(String fullPath, int count) throws SQLException {
//        final String sqlTemplate = batchInsertSql(fieldNames.size(), "%s");
//
//        try (FileWriter fileWriter = new FileWriter(fullPath)) {
//
//            for (int c = 0; c < count; c++) {
//                List<Object> rowData = new ArrayList<>();
//                for (int i = 1; i <= fieldTypes.size(); i++) {
//                    String fieldType = fieldTypes.get(i - 1);
//                    Object columnValue = generateData(fieldType);
//                    rowData.add(columnValue);
//                }
////                allData.add(rowData);
//                final String sqlWritten = String.format(sqlTemplate, rowData.toArray()) + ";\n";
//                fileWriter.write(sqlWritten);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public int columnSize() {
//        return fieldNames.size();
//    }

//    public int rowSize() {
//        return allData.size();
//    }

//    public Object[] getColumnData(int columnIndex) {
//        Object[] result = new Object[allData.size()];
//        int i = 0;
//        for (List<Object> row : allData) {
//            result[i++] = row.get(columnIndex);
//        }
//        return result;
//    }
//

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

    private String batchInsertSql(int columnSize, String placeHolder, Boolean withHint) {
        String hint = "/*+TDDL:cmd_extra(BATCH_INSERT_POLICY=\"NONE\")*/";
        String sql =
            String.format("insert into %s values ", tableName);
        StringJoiner joiner = new StringJoiner(",", "(", ")");
        for (int i = 0; i < columnSize; i++) {
            joiner.add(placeHolder);
        }
        sql += joiner.toString();
        if (withHint) {
            sql = hint + sql;
        }
        return sql;
    }

    private Object generateData(String fieldType) throws SQLException {
        return dataGenerator.generateData(fieldType);
    }

}
