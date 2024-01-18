package com.alibaba.polardbx.qatest.oss.ddl;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import org.junit.Ignore;

@Ignore
public class FileStorageModifyColumnTest extends FileStorageColumnDDLBaseTest {
    public FileStorageModifyColumnTest(String crossSchema, String seed) {
        super(crossSchema, "false", seed);
    }

    /**
     * | MODIFY [COLUMN] col_name column_definition
     * [FIRST | AFTER col_name]
     * <p>
     * Can change a column definition but not its name.
     * More convenient than CHANGE to change a column definition without renaming it.
     * With FIRST or AFTER, can reorder columns.
     */

    @Override
    protected void performDdl(String ddl) {
        if (StringUtil.isEmpty(ddl)) {
            return;
        }
        JdbcUtil.executeSuccess(getFullTypeConn(), String.format(ddl, fullTypeTable));
        JdbcUtil.executeSuccess(getCompareConn(), String.format(ddl, compareTable));
        JdbcUtil.executeSuccess(getInnoConn(), String.format(ddl, innodbTable));
    }
}
