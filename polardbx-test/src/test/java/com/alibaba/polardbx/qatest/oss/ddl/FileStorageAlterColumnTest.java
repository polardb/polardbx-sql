package com.alibaba.polardbx.qatest.oss.ddl;

import com.alibaba.polardbx.qatest.oss.utils.FullTypeSeparatedTestUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.runners.Parameterized;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil.C_BIGINT_1;
import static com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil.C_INT_1;
import static com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil.C_MEDIUMINT_24;
import static com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil.C_SMALLINT_1;
import static com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil.C_SMALLINT_16;
import static com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil.C_TINYINT_1;

public class FileStorageAlterColumnTest extends FileStorageColumnDDLBaseTest {

    /**
     * ALTER [COLUMN] col_name {
     * SET DEFAULT {literal | (expr)}
     * | DROP DEFAULT
     * }
     * <p>
     * Used only to change a column default value.
     */

    public FileStorageAlterColumnTest(String crossSchema, String seed) {
        super(crossSchema, "false", seed);
    }

    @Parameterized.Parameters(name = "{index}")
    public static List<String[]> prepareData() {
        return Arrays.asList(
            new String[] {"false", "1"},
            new String[] {"false", "2"}
        );
    }

    static class CurrentDefault {

        String columnName;

        boolean withDefault;

        int current;

        String columnType;

        public CurrentDefault(String columnName) {
            this.columnName = columnName;
            this.withDefault = true;
            this.current = 1;
            this.columnType = C_TINYINT_1;
        }

        public boolean isWithDefault() {
            return withDefault;
        }

        public void enableDefault() {
            this.withDefault = true;
        }

        public void disableDefault() {
            this.withDefault = false;
        }

        public void addCurrent() {
            this.current++;
        }

        void modifyColumn() {
            switch (columnType) {
            case C_TINYINT_1:
                this.columnType = C_SMALLINT_16;
                return;
            case C_SMALLINT_16:
                this.columnType = C_MEDIUMINT_24;
                return;
            case C_MEDIUMINT_24:
                this.columnType = C_INT_1;
                return;
            case C_INT_1:
                this.columnType = C_BIGINT_1;
                return;
            case C_BIGINT_1:
                // do nothing
                return;
            }
        }

        String getColumDef() {
            return FullTypeSeparatedTestUtil.allColumnDefinitions.get(columnType)
                .replace('`' + columnType + '`', " ");
        }
    }

    private List<CurrentDefault> columnDefaults = new ArrayList<>();

    @Override
    protected void realTest() {

        StringBuilder ddl;
        for (int round = 0; round < 10; round++) {
            ddl = new StringBuilder("alter table %s ");
            // drop default, set fault or modify column for all columns
            for (CurrentDefault currentDefault : columnDefaults) {
                currentDefault.enableDefault();
                switch (r1.nextInt(3)) {
                case 0:
                    // drop default
                    currentDefault.disableDefault();
                    ddl.append("alter column ").append(currentDefault.columnName).append(" drop default,");
                    break;
                case 1:
                    // set default
                    currentDefault.addCurrent();
                    ddl.append("alter column ").append(currentDefault.columnName).append(" set default ")
                        .append(currentDefault.current).append(",");
                    break;
                case 2:
                    // modify column
                    currentDefault.modifyColumn();
                    ddl.append("modify column ").append(currentDefault.columnName).append(currentDefault.getColumDef());
                    break;
                default:
                    // do nothing
                }
            }
            // perform ddl
            performDdl(ddl.toString());

            String nullColumnName =
                columnDefaults.stream().filter(x -> !x.isWithDefault()).map(x -> ", " + x.columnName)
                    .collect(Collectors.joining());

            String nullColumn = columnDefaults.stream().filter(x -> !x.isWithDefault()).map(x -> ", null")
                .collect(Collectors.joining());

            // add a new column
            String insertSql = "INSERT INTO %s(%s, gmt_modified%s) select %s,'%s'%s from %s.%s";

            Set<String> columns = getColumns();

            ddl = new StringBuilder("alter table %s add column ");

            String selectColumn = C_SMALLINT_1;
            String insertColumn = buildNewColumn(selectColumn, columns);
            String columnDef =
                FullTypeSeparatedTestUtil.allColumnDefinitions.get(selectColumn).replace(selectColumn, insertColumn)
                    .replace(",\n", "");
            ddl.append(columnDef);

            // perform ddl
            performDdl(ddl.toString());

            columnDefaults.add(new CurrentDefault(insertColumn));
            // insert
            for (LocalDate date : dateList) {
                JdbcUtil.executeSuccess(getCompareConn(),
                    String.format(insertSql, compareTable, insertColumn, nullColumnName, selectColumn, date, nullColumn,
                        getFullTypeSchema(),
                        fullTypeTable));
                JdbcUtil.executeSuccess(getInnoConn(),
                    String.format(insertSql, innodbTable, insertColumn, nullColumnName, selectColumn, date, nullColumn,
                        getFullTypeSchema(),
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
