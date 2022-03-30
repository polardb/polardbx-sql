package org.apache.calcite.sql;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlAlterTableGroupSplitPartitionByHotValue extends SqlAlterTableSplitPartitionByHotValue {

    private SqlAlterTableGroup parent;

    public SqlAlterTableGroupSplitPartitionByHotValue(SqlParserPos pos, List<SqlNode> hotkeys, SqlNode partitions,
                                                      SQLName hotKeyPartitionName) {
        super(pos, hotkeys, partitions, hotKeyPartitionName);
    }

    public SqlAlterTableGroup getParent() {
        return parent;
    }

    public void setParent(SqlAlterTableGroup parent) {
        this.parent = parent;
    }
}