package com.alibaba.polardbx.qatest.oss.ddl.index;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.collections.CollectionUtils;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class FileStorageCreateDropIndexAutoTest extends FileStorageAddDropIndexAutoTest {
    /**
     * | ADD {INDEX | KEY} [index_name]
     * [index_type] (key_part,...) [index_option] ...
     * | DROP {INDEX | KEY} index_name
     * | DROP PRIMARY KEY
     */

    public FileStorageCreateDropIndexAutoTest(String crossSchema, String seed) {
        super(crossSchema, seed);
        this.auto = true;
    }

    @Parameterized.Parameters(name = "{index}:cross={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(
            new String[] {"false", "7"},
            new String[] {"true", "8"}
        );
    }

    protected Set<String> buildIndexColumns(String[] columnArray) {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (int i = 0; i < 2; i++) {
            String column = columnArray[r1.nextInt(columnArray.length)];
            if (FileStorageCreateDropIndexAutoTest.IndexableColumns.contains(column)) {
                builder.add(column);
            }
        }
        return builder.build();
    }

    protected void addIndex(Set<String> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return;
        }
        String ddl = "create index index_" + (++indexCount) + " on %s ("
            + String.join(",", columns) + ")";
        performDdl(ddl);
    }

    protected String buildDDL1(String[] columnArray) {
        Set<String> indexColumns = buildIndexColumns(columnArray);
        if (CollectionUtils.isEmpty(indexColumns)) {
            return null;
        }
        StringBuilder ddl = new StringBuilder("create index ");
        if (!CollectionUtils.isEmpty(indexColumns)) {
            ddl.append("index_").append(++indexCount)
                .append(" on %s (").append(String.join(",", indexColumns)).append(")");
        }
        return ddl.toString();
    }

    protected String buildDDL2(String[] columnArray) {
        Set<String> indexes = getIndexes(getInnoConn(), innodbTable);
        if (indexes.isEmpty()) {
            return null;
        }
        String droppedIndex = (String) indexes.toArray()[r1.nextInt(indexes.size())];
        return " drop index " + droppedIndex + " on %s";
    }

}
