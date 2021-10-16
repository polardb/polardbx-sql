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

package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupMergePartitionPreparedData;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupMergePartition;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupMergePartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogicalAlterTableGroupRenamePartition extends BaseDdlOperation {

    public LogicalAlterTableGroupRenamePartition(DDL ddl) {
        super(ddl);
    }

    public static LogicalAlterTableGroupRenamePartition create(DDL ddl) {
        return new LogicalAlterTableGroupRenamePartition(ddl);
    }

}
