package com.alibaba.polardbx.qatest.oss.ddl;

import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.qatest.oss.utils.FullTypeSeparatedTestUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.runners.Parameterized;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class FileStorageAddDropColumnTest extends FileStorageColumnDDLBaseTest {

    /**
     * | ADD [COLUMN] col_name column_definition
     * [FIRST | AFTER col_name]
     * | ADD [COLUMN] (col_name column_definition,...)
     * | DROP [COLUMN] col_name
     */

    public FileStorageAddDropColumnTest(String crossSchema, String seed) {
        super(crossSchema, "false", seed);
    }

    @Parameterized.Parameters(name = "{index}:cross={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(
            new String[] {"false", "1"},
            new String[] {"true", "2"}
        );
    }

    @Override
    protected void realTest() {
        for (int round = 0; round < 10; round++) {
            // add and drop random column
            String insertSql = "INSERT INTO %s(%s, gmt_modified) select %s, '%s' from %s.%s";

            Set<String> columns = getColumns();
            String afterColumn = (String) columns.toArray()[r1.nextInt(columns.size())];
            String dropColumn = (String) columns.toArray()[r1.nextInt(columns.size())];

            StringBuilder ddl = new StringBuilder("alter table %s add column ");

            String selectColumn =
                allColumns.get(r1.nextInt(allColumns.size()));
            String insertColumn = buildNewColumn(selectColumn, columns);
            String columnDef =
                FullTypeSeparatedTestUtil.allColumnDefinitions.get(selectColumn).replace(selectColumn, insertColumn)
                    .replace(",\n", "");
            ddl.append(columnDef);

            switch (round % 3) {
            case 0:
                ddl.append(" first");
                break;
            case 1:
                ddl.append(" after ").append(afterColumn);
                if (StringUtils.equals(dropColumn, afterColumn)) {
                    dropColumn = null;
                }
                break;
            default:
                // do nothing
            }
            ddl.append(StringUtils.isEmpty(dropColumn) ? "" : ",drop " + dropColumn);

            // perform ddl
            performDdl(ddl.toString());

            // insert
            for (LocalDate date : dateList) {
                JdbcUtil.executeSuccess(getCompareConn(),
                    String.format(insertSql, compareTable, insertColumn, selectColumn, date, getFullTypeSchema(),
                        fullTypeTable));
                JdbcUtil.executeSuccess(getInnoConn(),
                    String.format(insertSql, innodbTable, insertColumn, selectColumn, date, getFullTypeSchema(),
                        fullTypeTable));
            }

            // expire local partition
            JdbcUtil.executeSuccess(getInnoConn(), String.format(EXPIRE, innodbTable, localPartitionQueue.poll()));

            // check correctness
            checkAgg();
            checkFilter();
        }
    }
}
