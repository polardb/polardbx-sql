/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.gsi;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * In INSERT IGNORE, we need to find which rows don't conflict with the existing
 * data in the database. In INSERT ON DUPLICATE KEY UPDATE, we need to find the
 * rows that conflict and the rows that do not. For the conflicting rows, we
 * also need to gather their corresponding conflicting data in the database. If
 * one row in the INSERT matches two records in the database, we need only one
 * record.
 */
public class DuplicateValueFinder {
    private final ExecutionContext ec;

    // GLOBAL INPUT MEMBERS

    // `duplicateValues` contains some columns in the SqlInsert, so which are
    // the unique keys? Find the indexes of unique keys in the columns.
    // Format: [[index of uk1_1, index of uk1_2], [index of uk2]]
    private List<List<Integer>> uniqueKeyIndexesInDuplicateValues;

    // `duplicateValues` contains some columns in the SqlInsert, so where are
    // the columns in the SqlInsert?
    // Format: [index of column1, index of column2]
    private List<Integer> duplicateValueIndexesInInsert;

    // DataTypes of the unique keys, each of which corresponds to
    // uniqueKeyIndexesInDuplicateValues
    private List<List<DataType>> uniqueKeyDataTypes;

    // PLAN LEVEL INPUT MEMBERS

    // Physical insert on the base table
    private PhyTableOperation baseInsertPlan;

    // Selected conflicting data in the database.
    // Format: [[value1 in row1, value2 in row1], [value1 in row2, ...]]
    private List<List<Object>> duplicateValues;

    // Row indexes of this physical insert to the logical insert
    private List<Integer> baseValueIndices;

    // PLAN LEVEL OUTPUT MEMBERS

    // Find distinct values in SqlInsert, which doesn't conflict with existing
    // data in the database.
    // Format: [row index in logical insert]
    private List<Integer> distinctValueIndices;

    // Now that we found conflicting values in database, we need to find which
    // rows in the SqlInsert does they match.
    // Format: [(row index in logical insert, conflicting values in database)]
    private List<Pair<Integer, List<Object>>> duplicateRows;

    public DuplicateValueFinder(TableMeta tableMeta, SqlNodeList targetColumnList, List<String> duplicateColumnNames,
                                ExecutionContext ec) {
        this.ec = ec;
        initKeyIndexes(tableMeta, targetColumnList, duplicateColumnNames);
    }

    public void setPhysicalPlan(PhyTableOperation baseInsertPlan, List<List<Object>> duplicateValues,
                                List<Integer> baseValueIndices) {
        this.baseInsertPlan = baseInsertPlan;
        this.duplicateValues = duplicateValues;
        this.baseValueIndices = baseValueIndices;

        this.distinctValueIndices = null;
        this.duplicateRows = null;
    }

    public void clearPhysicalPlan() {
        this.baseInsertPlan = null;
        this.duplicateValues = null;
        this.baseValueIndices = null;

        this.distinctValueIndices = null;
        this.duplicateRows = null;
    }

    public List<Integer> getDistinctValueIndices() {
        if (distinctValueIndices == null) {
            findDuplicate();
        }
        return distinctValueIndices;
    }

    public Collection<Pair<Integer, List<Object>>> getDuplicateRows() {
        if (duplicateRows == null) {
            findDuplicate();
        }
        return duplicateRows;
    }

    private void initKeyIndexes(TableMeta tableMeta, SqlNodeList targetColumnList, List<String> duplicateColumnNames) {
        List<List<String>> uniqueKeys = GlobalIndexMeta.getUniqueKeys(tableMeta, true, tm -> true, ec);
        uniqueKeyDataTypes = uniqueKeys.stream()
            .map(list -> list.stream()
                .map(name -> tableMeta.getColumnIgnoreCase(name).getDataType())
                .collect(Collectors.toList()))
            .collect(Collectors.toList());

        // uniqueKeys and duplicateColumnNames are both transformed to uppercase
        // already
        uniqueKeyIndexesInDuplicateValues = uniqueKeys.stream()
            .map(list -> list.stream().map(duplicateColumnNames::indexOf).collect(Collectors.toList()))
            .collect(Collectors.toList());

        List<String> columnNamesInInsert = targetColumnList.getList()
            .stream()
            .map(node -> ((SqlIdentifier) node).getLastName().toUpperCase())
            .collect(Collectors.toList());

        duplicateValueIndexesInInsert = duplicateColumnNames.stream()
            .map(columnNamesInInsert::indexOf)
            .collect(Collectors.toList());
    }

    /**
     * Why not pick rows directly from the logical insert by primary keys: there
     * might be same primary keys with different sharding keys in the logical
     * insert. Why not pick rows from the physical inserts and merge them:
     * parameter indexes would conflict, and we'll need to modify the indexes
     * recursively.
     */
    private void findDuplicate() {
        SqlInsert sqlInsert = (SqlInsert) baseInsertPlan.getNativeSqlNode();
        List<SqlNode> rows = ((SqlCall) sqlInsert.getSource()).getOperandList();

        Map<Integer, ParameterContext> oldParams = baseInsertPlan.getParam();
        List<List<Object>> valuesInInsert = new ArrayList<>(rows.size());
        for (SqlNode node : rows) {
            List<SqlNode> columns = ((SqlCall) node).getOperandList();
            List<Object> pickedValues = BuildPlanUtils.pickValuesFromRow(oldParams,
                columns,
                duplicateValueIndexesInInsert,
                false);
            valuesInInsert.add(pickedValues);
        }

        Map<Integer, List<Object>> duplicateRowsMap = new HashMap<>();

        for (int i = 0; i < uniqueKeyIndexesInDuplicateValues.size(); i++) {
            List<Integer> uniqueKeyIndexes = uniqueKeyIndexesInDuplicateValues.get(i);
            List<DataType> dataTypes = uniqueKeyDataTypes.get(i);
            List<List<Object>> outerValues = duplicateValues.stream()
                .map(allColumns -> uniqueKeyIndexes.stream().map(allColumns::get).collect(Collectors.toList()))
                .collect(Collectors.toList());

            for (int j = 0; j < valuesInInsert.size(); j++) {
                if (duplicateRowsMap.containsKey(j)) {
                    continue;
                }

                List<Object> innerValues = uniqueKeyIndexes.stream()
                    .map(valuesInInsert.get(j)::get)
                    .collect(Collectors.toList());
                int index = findIndexInValueList(outerValues, innerValues, dataTypes);
                if (index >= 0) {
                    // `j` is the index in SqlInsert, `index` is the index in
                    // duplicateValues
                    duplicateRowsMap.put(j, duplicateValues.get(index));
                }
            }
        }

        // transform to list and sort
        List<Map.Entry<Integer, List<Object>>> duplicateRowList = new ArrayList<>(duplicateRowsMap.entrySet());
        duplicateRowList.sort(Comparator.comparing(Map.Entry::getKey));
        this.duplicateRows = new ArrayList<>(duplicateRowList.size());
        this.distinctValueIndices = new ArrayList<>(rows.size());

        for (int i = 0, j = 0; i < baseValueIndices.size(); i++) {
            int valueIndexInBaseInsert = baseValueIndices.get(i);
            if (j >= duplicateRowList.size()) {
                distinctValueIndices.add(valueIndexInBaseInsert);
            } else {
                int duplicateIndex = duplicateRowList.get(j).getKey();
                if (i == duplicateIndex) {
                    duplicateRows.add(Pair.of(valueIndexInBaseInsert, duplicateRowList.get(j).getValue()));
                    j++;
                } else if (i < duplicateIndex) {
                    distinctValueIndices.add(valueIndexInBaseInsert);
                }
                // because keys in duplicateRowList are distinct, so there won't
                // be i > duplicateIndex
            }
        }
    }

    private static int findIndexInValueList(List<List<Object>> valueList, List<Object> values,
                                            List<DataType> dataTypes) {
        for (int i = 0; i < valueList.size(); i++) {
            if (compareValueList(valueList.get(i), values, dataTypes) == 0) {
                return i;
            }
        }
        return -1;
    }

    private static int compareValueList(List<Object> objects1, List<Object> objects2, List<DataType> dataTypes) {
        for (int i = 0; i < objects1.size(); i++) {
            int result = dataTypes.get(i).compare(objects1.get(i), objects2.get(i));
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

}
