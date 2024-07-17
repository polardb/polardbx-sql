package com.alibaba.polardbx.qatest.ddl.cdc.entity;

import com.alibaba.polardbx.cdc.CdcManager;
import com.alibaba.polardbx.cdc.entity.LogicMeta;
import com.alibaba.polardbx.common.cdc.entity.DDLExtInfo;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-12-11 18:26
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class DdlRecordInfo {
    private String id;
    private String jobId;
    private String ddlSql;
    private String schemaName;
    private String tableName;
    private String sqlKind;
    private int visibility;
    private DDLExtInfo ddlExtInfo;
    private CdcManager.MetaInfo metaInfo;

    public String getEffectiveSql() {
        return StringUtils.isNotBlank(ddlExtInfo.getOriginalDdl()) ? ddlExtInfo.getOriginalDdl() : ddlSql;
    }

    public Map<String, Set<String>> getTopology() {
        if (metaInfo != null) {
            LogicMeta.LogicTableMeta logicTableMeta = metaInfo.getLogicTableMeta();
            logicTableMeta.getPhySchemas().forEach(phySchema -> {
                Assert.assertTrue(org.apache.commons.lang.StringUtils.isNotBlank(phySchema.getSchema()));
            });
            return logicTableMeta.getPhySchemas().stream()
                .collect(Collectors.toMap(LogicMeta.PhySchema::getGroup,
                    v -> Sets.newHashSet(
                        v.getPhyTables().stream().map(String::toLowerCase).collect(Collectors.toList()))));
        }
        return Maps.newHashMap();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DdlRecordInfo that = (DdlRecordInfo) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
