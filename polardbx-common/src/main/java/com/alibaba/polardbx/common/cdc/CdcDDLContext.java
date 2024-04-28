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
    private Long versionId;
}
