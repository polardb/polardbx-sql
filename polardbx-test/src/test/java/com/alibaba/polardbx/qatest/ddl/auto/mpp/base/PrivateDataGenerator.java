package com.alibaba.polardbx.qatest.ddl.auto.mpp.base;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.qatest.util.JdbcUtil;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

public class PrivateDataGenerator {

//    public static void main(String[] args) {
//        List<String> columnNames = Arrays.asList("id", "partition", "name", "value");
//        List<String> columnTypes = Arrays.asList("int", "varchar(32)", "varchar(32)", "float");
//        List<Integer> pkColumnIndex = Arrays.asList(0);
//        List<Integer> partitionColumnIndex = Arrays.asList(1);
//        int rows = 10;
//
//        List<Map<String, Object>> data = generateDuplicatePkData(columnNames, columnTypes, pkColumnIndex, partitionColumnIndex, rows);
//
//        for (Map<String, Object> row : data) {
//            System.out.println(row);
//        }
//    }

    public static List<Map<String, Object>> generateDuplicatePkData(Connection connection, String schemaName,
                                                                    String tableName, List<String> columnNames,
                                                                    List<String> columnTypes,
                                                                    Integer pkColumnIndex,
                                                                    Integer partitionColumnIndex, int partNum,
                                                                    int eachPartRows, int depth) {
        if (depth > 3) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, " generate duplicate pk data failed!");
        }
        Random random = new Random();

        // 用于检查partition列对应数据的唯一性，确保PK列不同
        Map<String, Set<String>> partitionToPkMap = new HashMap<>();
        Map<String, Map<String, Object>> partitionPkToRowMap = new HashMap<>();
        List<Map<String, Object>> result = new ArrayList<>();
        String partitionKeyName = columnNames.get(partitionColumnIndex);
        String pkColumnName = columnNames.get(pkColumnIndex);
        Set<String> partRoutes = new HashSet<>();
        Long generateKeyCount = 0L;

        for (int i = 0; i < partNum; i++) {

            // 构建partition key
            String partitionValue;
            String part;
            do {
                partitionValue =
                    generateRandomValue(columnTypes.get(partitionColumnIndex), random);
                part = computePartRoute(connection, schemaName, tableName, partitionValue);
                generateKeyCount += 1;
            } while (partitionToPkMap.containsKey(partitionValue) || partRoutes.contains(part));
            partRoutes.add(part);

            for (int j = 0; j < eachPartRows; j++) {
                Map<String, Object> row = new LinkedHashMap<>();
                // 确保partition列对应的PK列唯一
                Set<String> usedPks = partitionToPkMap.computeIfAbsent(partitionValue, k -> new HashSet<>());
                String pk = "";
                for (int columnIndex = 0; columnIndex < columnNames.size(); columnIndex++) {
                    if (partitionColumnIndex == columnIndex) {
                        row.put(partitionKeyName, partitionValue);
                        continue; // partition列已经处理过
                    }

                    String value;
                    if (pkColumnIndex == columnIndex) {
                        // 对于PK列，需要确保唯一
                        do {
                            value = generateRandomValue(columnTypes.get(columnIndex), random);
                        } while (usedPks.contains(value));
                        pk = value;
                        usedPks.add(value);
                    } else {
                        value = generateRandomValue(columnTypes.get(columnIndex), random);
                    }

                    row.put(columnNames.get(columnIndex), value);
                }
                partitionPkToRowMap.put(partitionValue + "_" + pk, row);
                result.add(row);
            }
        }

        List<String> partitionKeys = partitionToPkMap.keySet().stream().collect(Collectors.toList());
        Random random1 = new Random();
        Boolean successFlag = false;
        for (int i = 0; i < 10; i++) {
            int partitionKeyIndex = random1.nextInt(partitionKeys.size());
            String selectPartitionKey = partitionKeys.get(partitionKeyIndex);
            Set<String> pks = partitionToPkMap.get(selectPartitionKey);
            String pk = randomSelectFromSet(pks);
            Map<String, Object> selectRow = partitionPkToRowMap.get(selectPartitionKey + "_" + pk);
            Map<String, Object> duplicateRow = new HashMap<>(selectRow);
            for (int j = 0; j < 10; j++) {
                int anotherPartitionKeyIndex = random1.nextInt(partitionKeys.size());
                if (anotherPartitionKeyIndex == partitionKeyIndex) {
                    continue;
                }
                String anotherPartitionKey = partitionKeys.get(anotherPartitionKeyIndex);
                if (partitionToPkMap.get(anotherPartitionKey).contains(duplicateRow.get(pkColumnName))) {
                    continue;
                }
                duplicateRow.put(partitionKeyName, partitionKeys.get(anotherPartitionKeyIndex));
                result.add(duplicateRow);
                successFlag = true;
                break;
            }
            if (successFlag) {
                break;
            }
        }
        if (!successFlag) {
            return generateDuplicatePkData(connection, schemaName, tableName, columnNames, columnTypes, pkColumnIndex,
                partitionColumnIndex, partNum,
                eachPartRows, depth + 1);

        }

        return result;
    }

    private static String generateRandomValue(String type, Random random) {
        switch (type.toLowerCase()) {
        case "int":
            return String.valueOf(random.nextInt(100000));
        case "bigint":
            return String.valueOf(random.nextLong());
        case "float":
            return String.valueOf(random.nextFloat());
        default:
            if (type.startsWith("varchar")) {
                int length = Integer.parseInt(type.replaceAll("[^0-9]", ""));
                return randomString(length, random);
            }
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    private static String randomString(int length, Random random) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder result = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            result.append(characters.charAt(random.nextInt(characters.length())));
        }

        return result.toString();
    }

    private static String randomSelectFromSet(Set<String> set) {
        int randomIndex = new Random().nextInt(set.size());
        Iterator<String> iterator = set.iterator();
        int currentIndex = 0;
        String result = "";
        while (iterator.hasNext()) {
            result = iterator.next();
            currentIndex++;
            if (currentIndex == randomIndex) {
                break;
            }
            currentIndex++;
        }
        return result;
    }

    static String computePartRoute(Connection connection, String schemaName, String tableName, String partitionValue) {
        String sql = String.format("select part_route(\"%s\", \"%s\", \"%s\")", schemaName, tableName, partitionValue);
        return JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(connection, sql)).get(0).get(0).toString();
    }
}
