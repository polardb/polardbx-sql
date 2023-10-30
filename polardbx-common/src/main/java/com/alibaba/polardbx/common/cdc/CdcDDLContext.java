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

package com.alibaba.polardbx.common.cdc;

import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.utils.Pair;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;
import java.util.Set;

@AllArgsConstructor
@Getter
public class CdcDDLContext {
    private final String schemaName;
    private final String tableName;
    private final String sqlKind;
    private final String ddlSql;
    private final DdlVisibility visibility;

    private final Long jobId;

    private final Long taskId;
    private final DdlType ddlType;
    private final Map<String, Object> extendParams;

    private final boolean isRefreshTableMetaInfo;

    private final Map<String, Set<String>> newTableTopology;

    private final Pair<String, TablesExtInfo> tablesExtInfoPair;
}
