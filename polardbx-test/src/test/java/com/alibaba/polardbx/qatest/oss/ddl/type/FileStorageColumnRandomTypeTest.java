package com.alibaba.polardbx.qatest.oss.ddl.type;

import com.alibaba.polardbx.qatest.oss.ddl.FileStorageColumnTypeTest;
import com.alibaba.polardbx.qatest.oss.utils.FileStorageTestUtil;
import com.alibaba.polardbx.qatest.oss.utils.FullTypeSeparatedTestUtil;
import com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FileStorageColumnRandomTypeTest extends FileStorageColumnTypeTest {

    public FileStorageColumnRandomTypeTest(String sourceCol, String targetCol) {
        super(sourceCol, targetCol, "false");
    }

    @Parameterized.Parameters(name = "{index}:source={0} target={1}")
    public static List<String[]> prepareData() {
        List<String[]> para = new ArrayList<>();
        List<List<String>> testCases = ImmutableList.of(
            SIGNED_INT_CONV,
            UNSIGNED_INT_CONV
        );
        for (List<String> columns : testCases) {
            for (int i = 0; i < columns.size(); i++) {
                for (int j = i + 1; j < columns.size(); j++) {
                    if (StringUtils.equals(columns.get(i), columns.get(j))) {
                        continue;
                    }
                    para.add(new String[] {columns.get(i), columns.get(j)});
                }
            }
        }
        return para;
    }

    @Test
    public void testFullTypeByColumn() {
        try {
            FullTypeSeparatedTestUtil.TypeItem typeItem = colItemMap.get(sourceCol);
            String innoTable = FullTypeSeparatedTestUtil.tableNameByColumn(sourceCol);
            String ossTable = "oss_" + innoTable;

            dropTableIfExists(getInnoConn(), innoTable);
            dropTableIfExists(getOssConn(), ossTable);

            JdbcUtil.executeSuccess(getInnoConn(), typeItem.getTableDefinition());

            // gen data by column-specific type item.
            Supplier<Object> generator1 = FullTypeTestUtil.getGenerator(sourceCol, false);
            // generate insert value & do insert
            List<Object> paramList = IntStream.range(0, BATCH_SIZE)
                .mapToObj(i -> generator1.get())
                .collect(Collectors.toList());
            insertData(paramList, innoTable, sourceCol);

            // clone an oss-table from innodb-table.
            FileStorageTestUtil.createLoadingTable(getOssConn(), ossTable, getInnoSchema(), innoTable, engine);

            // check
            checkAgg(innoTable, ossTable, sourceCol);
            checkFilter(innoTable, ossTable, sourceCol);
            // ddl
            StringBuilder ddl = new StringBuilder("alter table %s modify column ");
            String columnDef =
                FullTypeSeparatedTestUtil.allColumnDefinitions.get(targetCol).replace(targetCol, sourceCol)
                    .replace(",\n", "");
            ddl.append(columnDef);

            performDdl(getInnoConn(), ddl.toString(), innoTable);
            performDdl(getOssConn(), ddl.toString(), ossTable);
            // check
            checkAgg(innoTable, ossTable, sourceCol);
            checkFilter(innoTable, ossTable, sourceCol);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    protected void insertData(List<Object> paramList, String table, String col1) {
        String insertSql = String.format(INSERT_SQL_FORMAT, table, col1);
        List params = paramList.stream()
            .map(Collections::singletonList)
            .collect(Collectors.toList());

        JdbcUtil.updateDataBatch(getInnoConn(), insertSql, params);
    }
}
