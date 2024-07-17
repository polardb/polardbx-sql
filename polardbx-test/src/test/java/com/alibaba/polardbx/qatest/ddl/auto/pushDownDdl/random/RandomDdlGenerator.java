package com.alibaba.polardbx.qatest.ddl.auto.pushDownDdl.random;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

public class RandomDdlGenerator {
    String tableName;

    int columnBound = 0;

    int indexBound = 0;

    static int columnCount = 0;

    static int indexCount = 0;

    static final int NOT_CHANGE_NAME = 0;

    static final int CHANGE_NAME = 1;

    final static Log log = LogFactory.getLog(RandomDdlGenerator.class);

    static List<String> supportedTypes = Arrays.asList("int", "bigint", "smallint", "varchar(10)", "varchar(20)",
        "double");
    static Random random = new Random();

    @Data
    class ColumnDef {
        private int id;
        private String name;
        private String type;
        private String defaultValue;
        private String encoding;

        public ColumnDef(int id, String name, String type, String defaultValue, String encoding) {
            this.id = id;
            this.name = name;
            this.type = type;
            this.defaultValue = defaultValue;
            this.encoding = encoding;
        }

        // Getters and setters...
        // toString() method...
    }

    @Data
    class IndexDef {
        private int id;
        private String name;
        private List<String> columns;

        public IndexDef(int id, String name, List<String> columns) {
            this.id = id;
            this.name = name;
            this.columns = columns;
        }
    }

    @Data
    class TableDef {
        public List<ColumnDef> columnDefs;
        public List<IndexDef> indexDefs;

        public TableDef(List<ColumnDef> columnDefs, List<IndexDef> indexDefs) {
            this.columnDefs = columnDefs;
            this.indexDefs = indexDefs;
        }

    }

    public RandomDdlGenerator(String tableName) {
        this.tableName = tableName;
    }

    public String generateRandomAlterDdlForCreateTableStmt(String originalSql) {
        List<ColumnDef> columnDefs = new ArrayList<>();
        List<IndexDef> indexDefs = new ArrayList<>();
        if (!StringUtils.isEmpty(originalSql)) {
            final MySqlCreateTableStatement createTableStmt =
                (MySqlCreateTableStatement) SQLUtils.parseStatementsWithDefaultFeatures(originalSql,
                        JdbcConstants.MYSQL).get(0)
                    .clone();
            List<SQLColumnDefinition> columnDefinitions = createTableStmt.getColumnDefinitions();
            List<MySqlKey> indexDefinitions = createTableStmt.getMysqlKeys();
            for (int i = 0; i < columnDefinitions.size(); i++) {
                columnDefs.add(new ColumnDef(i, columnDefinitions.get(i).getColumnName(),
                    columnDefinitions.get(i).getDataType().toString(),
                    columnDefinitions.get(i).getDefaultExpr().toString(),
                    ""
                ));
            }
            for (int i = 0; i < indexDefinitions.size(); i++) {
                indexDefs.add(new IndexDef(i, indexDefinitions.get(i).getName().toString(),
                    indexDefinitions.get(i).getColumns().stream().map(o -> o.toString()).collect(Collectors.toList())));
            }
        } else {
            // 定义MySQL表对应的列定义List<ColumnDef>
            columnDefs.add(new ColumnDef(1, "id", "INT", "0", "UTF-8"));
            columnDefs.add(new ColumnDef(2, "name", "VARCHAR(50)", null, "UTF-8"));
            columnDefs.add(new ColumnDef(3, "age", "INT", "0", "UTF-8"));
            columnDefs.add(new ColumnDef(4, "email", "VARCHAR(100)", null, "UTF-8"));
        }

        columnBound = columnDefs.size() + 1;
        indexBound = indexDefs.size() + 1;
        TableDef tableDef = new TableDef(columnDefs, indexDefs);
        // 生成随机的新表
        TableDef newTableDef = generateRandomTable(tableDef);
        int newColumnNum = newTableDef.columnDefs.size();
        while (newColumnNum <= 0) {
            newTableDef = generateRandomTable(tableDef);
        }

        List<String> alterStmts = generateAlterTableDefDdl(tableDef, newTableDef);
//        Collections.shuffle(alterStmts);
        log.info("original table def " + tableDef);
        log.info("generate new table def " + newTableDef);
        String alterDdl = String.format("ALTER TABLE %s %s;", tableName, StringUtils.join(alterStmts, ","));
        return alterDdl;
    }

    List<String> generateAlterTableDefDdl(TableDef tableDef, TableDef newTableDef) {
        List<String> alterStmts = new ArrayList<>();
        alterStmts.addAll(generateAlterTableColumnDdl(tableDef.columnDefs, newTableDef.columnDefs));
        alterStmts.addAll(generateAlterTableIndexDdl(tableDef.indexDefs, newTableDef.indexDefs));
        return alterStmts;
    }

    public List<String> generateAlterTableColumnDdl(List<ColumnDef> oldColumnDefs, List<ColumnDef> newColumnDefs) {
        Map<Integer, Integer> oldColumnDefMap = new HashMap<>();
        for (int i = 0; i < oldColumnDefs.size(); i++) {
            oldColumnDefMap.put(oldColumnDefs.get(i).id, i);
        }
        Map<Integer, Integer> newColumnDefMap = new HashMap<>();
        for (int i = 0; i < newColumnDefs.size(); i++) {
            newColumnDefMap.put(newColumnDefs.get(i).id, i);
        }

        List<Integer> dropColumnIds = new ArrayList<>();
        List<Integer> addColumnIds = new ArrayList<>();
        List<Integer> changeColumnIds = new ArrayList<>();
        List<Integer> modifyColumnIds = new ArrayList<>();

        for (ColumnDef columnDef : oldColumnDefs) {
            int columnId = columnDef.getId();
            if (!newColumnDefMap.containsKey(columnId)) {
                dropColumnIds.add(columnId);
            } else {
                int newColumnIndex = newColumnDefMap.get(columnId);
                ColumnDef newColumnDef = newColumnDefs.get(newColumnIndex);
                switch (diffColumn(columnDef, newColumnDef)) {
                case NOT_CHANGE_NAME:
                    modifyColumnIds.add(columnId);
                    break;
                case CHANGE_NAME:
                    changeColumnIds.add(columnId);
                    break;
                }
            }
        }
        for (ColumnDef columnDef : newColumnDefs) {
            int columnId = columnDef.getId();
            if (!oldColumnDefMap.containsKey(columnId)) {
                addColumnIds.add(columnId);
            }
        }
        String addColumnTemplate = "ADD COLUMN %s %s %s";
        String dropColumnTemplate = "DROP COLUMN %s";
        String changeColumnTemplate = "CHANGE COLUMN %s %s %s %s";
        String modifyColumnTemplate = "MODIFY COLUMN %s %s %s";
        List<String> alterStmts = new ArrayList<>();

        for (Integer columnId : addColumnIds) {
            int columnIndex = newColumnDefMap.get(columnId);
            String columnName = newColumnDefs.get(columnIndex).name;
            String columnType = newColumnDefs.get(columnIndex).getType();
            String oldColumnName = "";
            String afterColumnName = "";
            if (columnIndex == 0) {
                afterColumnName = "FIRST";
            } else {
                afterColumnName = "AFTER " + newColumnDefs.get(columnIndex - 1).name;
            }
            String alterStmt =
                applyColumnDefToTemplate(addColumnTemplate, columnName, columnType, oldColumnName, afterColumnName);
            alterStmts.add(alterStmt);
        }
        for (Integer columnId : dropColumnIds) {
            int columnIndex = oldColumnDefMap.get(columnId);
            String columnName = oldColumnDefs.get(columnIndex).name;
            String columnType = oldColumnDefs.get(columnIndex).getType();
            String oldColumnName = "";
            String afterColumnName = "";
            String alterStmt =
                applyColumnDefToTemplate(dropColumnTemplate, columnName, columnType, oldColumnName, afterColumnName);
            alterStmts.add(alterStmt);
        }
        for (Integer columnId : modifyColumnIds) {
            int columnIndex = newColumnDefMap.get(columnId);
            String columnName = newColumnDefs.get(columnIndex).name;
            String columnType = newColumnDefs.get(columnIndex).getType();
            String oldColumnName = "";
            String afterColumnName = "";
            if (columnIndex == 0) {
                afterColumnName = "FIRST";
            } else {
                afterColumnName = "AFTER " + newColumnDefs.get(columnIndex - 1).name;
            }
            String alterStmt =
                applyColumnDefToTemplate(modifyColumnTemplate, columnName, columnType, oldColumnName, afterColumnName);
            alterStmts.add(alterStmt);
        }
        for (Integer columnId : changeColumnIds) {
            int columnIndex = newColumnDefMap.get(columnId);
            String columnName = newColumnDefs.get(columnIndex).name;
            String columnType = newColumnDefs.get(columnIndex).getType();
            String oldColumnName = oldColumnDefs.get(oldColumnDefMap.get(columnId)).name;
            String afterColumnName = "";
            if (columnIndex == 0) {
                afterColumnName = "FIRST";
            } else {
                afterColumnName = "AFTER " + newColumnDefs.get(columnIndex - 1).name;
            }
            String alterStmt =
                applyColumnDefToTemplate(changeColumnTemplate, columnName, columnType, oldColumnName, afterColumnName);
            alterStmts.add(alterStmt);
        }
        return alterStmts;
    }

    public List<String> generateAlterTableIndexDdl(List<IndexDef> oldIndexDefs, List<IndexDef> newIndexDefs) {
        Map<Integer, Integer> oldIndexDefMap = new HashMap<>();
        for (int i = 0; i < oldIndexDefs.size(); i++) {
            oldIndexDefMap.put(oldIndexDefs.get(i).id, i);
        }
        Map<Integer, Integer> newIndexDefMap = new HashMap<>();
        for (int i = 0; i < newIndexDefs.size(); i++) {
            newIndexDefMap.put(newIndexDefs.get(i).id, i);
        }

        List<Integer> dropIndexIds = new ArrayList<>();
        List<Integer> addIndexIds = new ArrayList<>();
        List<Integer> renameIndexIds = new ArrayList<>();

        for (IndexDef indexDef : oldIndexDefs) {
            int indexId = indexDef.getId();
            if (!newIndexDefMap.containsKey(indexId)) {
                dropIndexIds.add(indexId);
            } else {
                int newIndexIndex = newIndexDefMap.get(indexId);
                IndexDef newIndexDef = newIndexDefs.get(newIndexIndex);
                switch (diffIndex(indexDef, newIndexDef)) {
                case NOT_CHANGE_NAME:
                    break;
                case CHANGE_NAME:
                    renameIndexIds.add(indexId);
                    break;
                }
            }
        }
        for (IndexDef indexDef : newIndexDefs) {
            int indexId = indexDef.getId();
            if (!oldIndexDefMap.containsKey(indexId)) {
                addIndexIds.add(indexId);
            }
        }
        String addIndexTemplate = "ADD LOCAL INDEX %s(%s)";
        String dropIndexTemplate = "DROP INDEX %s";
        String renameIndexTemplate = "RENAME INDEX %s to %s";
        List<String> alterStmts = new ArrayList<>();

        for (Integer indexId : addIndexIds) {
            int indexIndex = newIndexDefMap.get(indexId);
            String indexName = newIndexDefs.get(indexIndex).name;
            List<String> columnList = newIndexDefs.get(indexIndex).getColumns();
            String alterStmt =
                String.format(addIndexTemplate, indexName, StringUtils.join(columnList, ","));
            alterStmts.add(alterStmt);
        }
        for (Integer indexId : dropIndexIds) {
            int indexIndex = oldIndexDefMap.get(indexId);
            String indexName = oldIndexDefs.get(indexIndex).name;
            String alterStmt =
                String.format(dropIndexTemplate, indexName);
            alterStmts.add(alterStmt);
        }
        for (Integer indexId : renameIndexIds) {
            int newIndexIndex = newIndexDefMap.get(indexId);
            int oldIndexIndex = oldIndexDefMap.get(indexId);
            String oldIndexName = oldIndexDefs.get(oldIndexIndex).name;
            String newIndexName = newIndexDefs.get(newIndexIndex).name;
            String alterStmt =
                String.format(renameIndexTemplate, oldIndexName, newIndexName);
            alterStmts.add(alterStmt);
        }
        return alterStmts;
    }

    String applyColumnDefToTemplate(String template, String columnName, String columnType, String oldColumnName,
                                    String afterColumName) {
        if (template.startsWith("ADD")) {
            return String.format(template, columnName, columnType, afterColumName);
        } else if (template.startsWith("DROP")) {
            return String.format(template, columnName);
        } else if (template.startsWith("CHANGE")) {
            return String.format(template, oldColumnName, columnName, columnType, afterColumName);
        } else if (template.startsWith("MODIFY")) {
            return String.format(template, columnName, columnType, afterColumName);
        } else {
            return "";
        }
    }

    private int diffColumn(ColumnDef columnDef1, ColumnDef columnDef2) {
        if (columnDef1.name.equalsIgnoreCase(columnDef2.name)) {
            return NOT_CHANGE_NAME;
        } else {
            return CHANGE_NAME;
        }
    }

    private int diffIndex(IndexDef indexDef1, IndexDef indexDef2) {
        if (indexDef1.name.equalsIgnoreCase(indexDef2.name)) {
            return NOT_CHANGE_NAME;
        } else {
            return CHANGE_NAME;
        }
    }

    private TableDef generateRandomTable(TableDef tableDef) {
        List<ColumnDef> newColumnDefs = generateNewColumnDefs(tableDef.columnDefs);
        List<IndexDef> newIndexDefs = generateNewIndexDefs(tableDef.indexDefs, newColumnDefs);
        return new TableDef(newColumnDefs, newIndexDefs);
    }

    private List<IndexDef> generateNewIndexDefs(List<IndexDef> indexDefs, List<ColumnDef> columnDefs) {
        List<IndexDef> newIndexDefs = new ArrayList<>();
        List<IndexDef> indexToKeeps = new ArrayList<>(indexDefs);
        List<String> indexNames = new ArrayList<>();
        Random random = new Random();
        // 随机舍弃一些索引
        int indexesToDiscard = random.nextInt(indexDefs.size() + 1);
        for (int i = 0; i < indexesToDiscard; i++) {
            int indexToRemove = random.nextInt(indexToKeeps.size());
            indexNames.add(indexToKeeps.get(indexToRemove).getName());
            indexToKeeps.remove(indexToRemove);
        }

        int swapIndexNum = random.nextInt(indexToKeeps.size() + 1);
        if (swapIndexNum > 0) {
            Collections.shuffle(indexToKeeps);
            List<IndexDef> indexToShuffle = indexToKeeps.subList(0, swapIndexNum);
            indexToKeeps = indexToKeeps.subList(swapIndexNum, indexToKeeps.size());
            List<String> shuffleIndexNames = indexToShuffle.stream().map(o -> o.getName()).collect(Collectors.toList());
            for (IndexDef indexDef : indexToShuffle) {
                IndexDef modifiedIndexDef = modifyIndexName(indexDef);
                modifiedIndexDef.setName(randomSelectAndDelete(shuffleIndexNames));
                newIndexDefs.add(modifiedIndexDef);
            }
        }

        // 随机修改一些索引名字
        for (IndexDef indexDef : indexToKeeps) {
            IndexDef modifiedIndexDef = modifyIndexName(indexDef);
            if (indexNames.size() > 0 && random.nextBoolean()) {
                modifiedIndexDef.setName(randomSelectAndDelete(indexNames));
            }
            if (!modifiedIndexDef.getName().equalsIgnoreCase(indexDef.getName())) {
                indexNames.add(indexDef.getName());
            }
            newIndexDefs.add(modifiedIndexDef);
        }

        // 随机生成1~5个索引
        int indexesToAdd = random.nextInt(5) + 1; // 随机生成1-5个索引
        for (int i = 0; i < indexesToAdd; i++) {
            IndexDef newIndexDef = generateRandomIndexDef(columnDefs);
            newIndexDefs.add(newIndexDef);
        }
        return newIndexDefs;
    }

    private String generateRandomIndexName() {
        indexCount += 1;
        return String.format("i_%d", indexCount);
    }

    private IndexDef modifyIndexName(IndexDef indexDef) {
        String randomIndexName = generateRandomIndexName();
        IndexDef newIndexDef = new IndexDef(indexDef.id, randomIndexName, indexDef.columns);
        return newIndexDef;
    }

    private IndexDef generateRandomIndexDef(List<ColumnDef> columnDefs) {
        Random random = new Random();
        int id = indexBound;
        indexBound += 1;
        String name = generateRandomIndexName();
        List<String> columns = columnDefs.stream().map(o -> o.name).collect(Collectors.toList());
        int columnNum = random.nextInt(columns.size() + 1);
        while (columnNum == 0) {
            columnNum = random.nextInt(columns.size() + 1);
        }
        List<String> columnList = new ArrayList<>();
        while (columnList.size() < columnNum) {
            String column = randomSelectAndDelete(columns);
            columnList.add(column);
        }
        return new IndexDef(id, name, columnList);
    }

    private List<ColumnDef> generateNewColumnDefs(List<ColumnDef> columnDefs) {
        List<ColumnDef> newColumnDefs = new ArrayList<>();
        List<ColumnDef> columnsToKeep = new ArrayList<>(columnDefs);
        List<String> columnNames = new ArrayList<>();
        Random random = new Random();
        // 随机舍弃一些列
        int columnsToDiscard = random.nextInt(columnDefs.size() + 1);
        for (int i = 0; i < columnsToDiscard; i++) {
            int indexToRemove = random.nextInt(columnsToKeep.size());
            columnNames.add(columnsToKeep.get(indexToRemove).getName());
            columnsToKeep.remove(indexToRemove);
        }

        // 随机交换一批列的名字并修改其类型
        int swapColumnNum = random.nextInt(columnsToKeep.size() + 1);
        if (swapColumnNum > 0) {
            Collections.shuffle(columnsToKeep);
            List<ColumnDef> columnsToShuffle = columnsToKeep.subList(0, swapColumnNum);
            columnsToKeep = columnsToKeep.subList(swapColumnNum, columnsToKeep.size());
            List<String> shuffleColumnNames =
                columnsToShuffle.stream().map(o -> o.getName()).collect(Collectors.toList());
            for (ColumnDef columnDef : columnsToShuffle) {
                ColumnDef modifiedColumnDef = modifyColumnDef(columnDef);
                modifiedColumnDef.setName(randomSelectAndDelete(shuffleColumnNames));
                newColumnDefs.add(modifiedColumnDef);
            }
        }
        // 随机修改一些列的名字或类型
        for (ColumnDef columnDef : columnsToKeep) {
            ColumnDef modifiedColumnDef = modifyColumnDef(columnDef);
            if (columnNames.size() > 0 && random.nextBoolean()) {
                modifiedColumnDef.setName(randomSelectAndDelete(columnNames));
            }
            if (!columnDef.getName().equalsIgnoreCase(modifiedColumnDef.getName())) {
                columnNames.add(columnDef.getName());
            }
            newColumnDefs.add(modifiedColumnDef);
        }

        // 随机生成1~5个新列
        int columnsToAdd = random.nextInt(5) + 1; // 随机生成1-5列
        for (int i = 0; i < columnsToAdd; i++) {
            ColumnDef newColumn = generateRandomColumnDef();
            // TODO(taokun): this is for drop_and_insert_column_case.
//            if(columnNames.size() > 0 && random.nextBoolean()) {
//                newColumn.setName(randomSelectAndDelete(columnNames));
//            }
            newColumnDefs.add(newColumn);
        }

        // 随机定义列在新表List中的位置
        List<ColumnDef> finalColumnDefs = new ArrayList<>(newColumnDefs);
        Collections.shuffle(finalColumnDefs);
        return finalColumnDefs;
    }

    private String randomSelectAndDelete(List<String> columnNames) {
        Random random = new Random();
        int selectedIndex = random.nextInt(columnNames.size());
        String name = columnNames.get(selectedIndex);
        columnNames.remove(selectedIndex);
        return name;
    }

    private ColumnDef generateRandomColumnDef() {
        Random random = new Random();
        int id = columnBound;
        columnBound += 1;
        String name = generateRandomColumnName();
        String type = generateRandomType();
        String defaultValue = random.nextBoolean() ? "0" : null;
        String encoding = "UTF-8";
        return new ColumnDef(id, name, type, defaultValue, encoding);
    }

    private String generateRandomColumnName() {
        columnCount += 1;
        return String.format("c%d", columnCount);
    }

    private String generateRandomType() {
        int randomIndex = random.nextInt(supportedTypes.size());
        return supportedTypes.get(randomIndex);
    }

    private ColumnDef modifyColumnDef(ColumnDef columnDef) {
        Random random = new Random();
        int id = columnDef.getId();
        String name = columnDef.getName();
        String type = columnDef.getType();
        String defaultValue = columnDef.getDefaultValue();
        String encoding = columnDef.getEncoding();

        // 随机修改列的名字或类型
        if (random.nextBoolean()) {
            name = generateRandomColumnName();
        }
        if (random.nextBoolean()) {
            type = generateRandomType();
        }
        return new ColumnDef(id, name, type, defaultValue, encoding);
    }

}
