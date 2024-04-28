package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import lombok.Getter;
import org.apache.calcite.sql.SqlCheckColumnarIndex.CheckCciExtraCmd;

import java.util.EnumSet;

@Getter
public class CheckCciPrepareData extends DdlPreparedData {

    String indexName;
    CheckCciExtraCmd extraCmd;

    public CheckCciPrepareData(String schemaName, String tableName, String indexName, CheckCciExtraCmd extraCmd) {
        super(schemaName, tableName);
        this.indexName = indexName;
        this.extraCmd = extraCmd;
    }

    /**
     * @return String like "`index` on `schema`.`table`"
     */
    public String humanReadableIndexName() {
        return "`" + indexName + '`'
            + (null == getTableName() ? "" : " ON `" + getSchemaName() + "`.`"
            + getTableName() + '`');
    }

    public boolean isClear() {
        return this.extraCmd == CheckCciExtraCmd.CLEAR;
    }

    public boolean isShow() {
        return this.extraCmd == CheckCciExtraCmd.SHOW;
    }

    public boolean isCheck() {
        return EnumSet
            .of(CheckCciExtraCmd.DEFAULT, CheckCciExtraCmd.CHECK, CheckCciExtraCmd.LOCK)
            .contains(this.extraCmd);
    }

    /**
     * Check metadata only
     */
    public boolean isMeta() {
        return this.extraCmd == CheckCciExtraCmd.META;
    }

    public boolean isNeedReport(boolean asyncDdlMode) {
        return isShow() || isMeta() || (!asyncDdlMode && isCheck());
    }
}
