package com.alibaba.polardbx.qatest.oss.ddl;

public class FileStorageChangeColumnTest {
    /**
     * | CHANGE [COLUMN] old_col_name new_col_name column_definition
     *         [FIRST | AFTER col_name]
     * <p>
     * Can rename a column and change its definition, or both.
     * Has more capability than MODIFY or RENAME COLUMN, but at the expense of convenience for some operations.
     *      CHANGE requires naming the column twice if not renaming it,
     *      and requires respecifying the column definition if only renaming it.
     * With FIRST or AFTER, can reorder columns.
     */
}
