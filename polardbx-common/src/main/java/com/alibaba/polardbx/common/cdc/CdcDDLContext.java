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

import com.alibaba.polardbx.common.ddl.Job;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.utils.Pair;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.Map;
import java.util.Set;

/**
 * Created by ziyang.lb
 **/
@RequiredArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class CdcDDLContext {
    private final String schemaName;
    private final String tableName;
    private final String sqlKind;
    private final String ddlSql;
    private final CdcDdlMarkVisibility visibility;
    /**
     * 新老引擎都有jobId
     */
    private final Long jobId;
    /**
     * 新引擎专用
     */
    private final Long taskId;
    private final DdlType ddlType;
    private final boolean newDdlEngine;
    private final Map<String, Object> extendParams;
    /**
     * 老引擎专用
     */
    private final Job job;
    /**
     * 打标时是否需要重刷拓扑元数据信息
     */
    private final boolean refreshTableMetaInfo;
    /**
     * 新的拓扑元数据信息
     */
    private final Map<String, Set<String>> newTableTopology;

    private final Pair<String, TablesExtInfo> tablesExtInfoPair;
    private final Long versionId;
    /**
     * 提交的tso结果，加了CDC_MARK_RECORD_COMMIT_TSO标记并成功执行才会存储
     */

    private Long commitTso;

    private boolean sequenceDdl;
}
