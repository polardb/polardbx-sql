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

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCheckGsi;
import lombok.Value;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Pair;

/**
 * @author moyi
 * @since 2021/07
 */
@Value
public class CheckGsiPrepareData extends DdlPreparedData {

    String indexName;
    String extraCmd;
    Pair<SqlSelect.LockMode, SqlSelect.LockMode> lockMode;
    long batchSize;
    long speedMin;
    long speedLimit;
    long parallelism;
    long earlyFailNumber;
    boolean useBinary;

    public String prettyTableName() {
        return "`" + indexName + '`'
            + (null == getTableName() ? "" : " ON `" + getSchemaName() + "`.`"
            + getTableName() + '`');
    }

    public boolean isClear() {
        return "clear".equalsIgnoreCase(this.extraCmd);
    }

    public boolean isShow() {
        return "show".equalsIgnoreCase(this.extraCmd);
    }

    public boolean isCorrect() {
        return this.extraCmd != null && LogicalCheckGsi.CorrectionType.oneOf(this.extraCmd);
    }

    public boolean isCheck() {
        return TStringUtil.isBlank(this.extraCmd)
            || "check".equalsIgnoreCase(this.extraCmd)
            || "lock".equals(this.extraCmd);
    }

}
