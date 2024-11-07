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

package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import lombok.Getter;
import org.apache.calcite.sql.SqlCheckColumnarIndex.CheckCciExtraCmd;

import java.util.EnumSet;
import java.util.List;

@Getter
public class CheckCciPrepareData extends DdlPreparedData {

    String indexName;
    CheckCciExtraCmd extraCmd;
    List<Long> tsoList;

    public CheckCciPrepareData(String schemaName, String tableName, String indexName, CheckCciExtraCmd extraCmd,
                               List<Long> tsoList) {
        super(schemaName, tableName);
        this.indexName = indexName;
        this.extraCmd = extraCmd;
        this.tsoList = tsoList;
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

    public boolean isIncrement() {
        return this.extraCmd == CheckCciExtraCmd.INCREMENT;
    }

    public boolean isSnapshot() {
        return this.extraCmd == CheckCciExtraCmd.SNAPSHOT;
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
        return isShow() || isMeta()
            || (!asyncDdlMode && isCheck())
            || (!asyncDdlMode && isIncrement())
            || (!asyncDdlMode && isSnapshot());
    }
}
