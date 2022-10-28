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

package com.alibaba.polardbx.executor.mpp.split;

import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.UnionBytesSql;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.SerializeUtils;
import com.alibaba.polardbx.executor.mpp.spi.ConnectorSplit;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableScanBuilder;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

public class JdbcSplit implements ConnectorSplit {

    private final String catalogName;
    private final String schemaName;
    private final String dbIndex;
    private String hostAddress;
    private List<List<String>> tableNames;
    private ITransaction.RW rw;
    private boolean containSelect;
    private Long intraGroupSortKey;

    protected final byte[] hint;
    protected final BytesSql sqlTemplate;
    protected final String orderBy;
    protected transient List<ParameterContext> flattenParams;
    protected transient String hintSql;
    protected transient long limit = -1;
    private transient List<List<ParameterContext>> params;

    /**
     * For galaxy protocol.
     */
    protected final byte[] galaxyDigest;
    protected boolean supportGalaxyPrepare; // will set to false if dynamic or stream jdbc split

    public JdbcSplit(
        String catalogName,
        String schemaName,
        String dbIndex,
        byte[] hint,
        BytesSql sqlTemplate,
        String orderBy,
        List<List<ParameterContext>> params,
        String hostAddress,
        List<List<String>> tableNames,
        ITransaction.RW rw,
        boolean containSelect,
        Long intraGroupSortKey,
        byte[] galaxyDigest,
        boolean supportGalaxyPrepare) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.dbIndex = dbIndex;
        this.hint = hint;
        this.params = params;
        this.hostAddress = hostAddress;
        this.tableNames = tableNames;
        this.rw = rw;
        this.sqlTemplate = sqlTemplate;
        this.orderBy = orderBy;
        this.containSelect = containSelect;
        this.intraGroupSortKey = intraGroupSortKey;
        this.galaxyDigest = galaxyDigest;
        this.supportGalaxyPrepare = supportGalaxyPrepare;
    }

    @JsonCreator
    public JdbcSplit(
        @JsonProperty("catalogName") String catalogName,
        @JsonProperty("schemaName") String schemaName,
        @JsonProperty("dbIndex") String dbIndex,
        @JsonProperty("hint") byte[] hint,
        @JsonProperty("sqlTemplate") BytesSql sqlTemplate,
        @JsonProperty("orderBy") String orderBy,
        @JsonProperty("params") List<List<ParameterContext>> params,
        @JsonProperty("hostAddress") String hostAddress,
        @JsonProperty("tableNames") List<List<String>> tableNames,
        @JsonProperty("rw") String rw,
        @JsonProperty("containSelect") boolean containSelect,
        @JsonProperty("galaxyDigest") byte[] galaxyDigest,
        @JsonProperty("supportGalaxyPrepare") boolean supportGalaxyPrepare) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.dbIndex = dbIndex;
        this.hint = hint;
        this.params = params;
        this.hostAddress = hostAddress;
        this.tableNames = tableNames;
        this.rw = ITransaction.RW.valueOf(rw.toUpperCase());
        this.sqlTemplate = sqlTemplate;
        this.orderBy = orderBy;
        this.containSelect = containSelect;
        this.galaxyDigest = galaxyDigest;
        this.supportGalaxyPrepare = supportGalaxyPrepare;
    }

    public JdbcSplit(JdbcSplit jdbcSplit) {
        this(jdbcSplit.getCatalogName(), jdbcSplit.getSchemaName(), jdbcSplit.getDbIndex(), jdbcSplit.getHint(),
            jdbcSplit.getSqlTemplate(), jdbcSplit.getOrderBy(),
            jdbcSplit.getParams(), jdbcSplit.getHostAddress(), jdbcSplit.getTableNames(), jdbcSplit.getTransactionRw(),
            jdbcSplit.isContainSelect(), jdbcSplit.getIntraGroupSortKey(), jdbcSplit.getGalaxyDigest(),
            jdbcSplit.isSupportGalaxyPrepare());
    }

    @JsonProperty("rw")
    public String getRw() {
        return rw.toString();
    }

    @JsonIgnore
    public ITransaction.RW getTransactionRw() {
        return rw;
    }

    @Override
    @JsonProperty
    public String getHostAddress() {
        return hostAddress;
    }

    @Override
    @JsonIgnore
    public Object getInfo() {
        return this;
    }

    @JsonProperty
    public String getCatalogName() {
        return catalogName;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getDbIndex() {
        return dbIndex;
    }

    @JsonProperty
    public byte[] getHint() {
        return hint;
    }

    @JsonProperty
    public BytesSql getSqlTemplate() {
        return sqlTemplate;
    }

    @JsonProperty
    public String getOrderBy() {
        return orderBy;
    }

    @JsonIgnore
    public BytesSql getUnionBytesSql(boolean ignore) {
        return new UnionBytesSql(sqlTemplate.getBytesArray(), sqlTemplate.isParameterLast(), tableNames.size(),
            orderBy == null ? null : orderBy.getBytes(),
            limit > 0 ? (limit + "").getBytes() : null);
    }

    @JsonProperty
    public boolean isContainSelect() {
        return containSelect;
    }

    @JsonProperty
    public byte[] getGalaxyDigest() {
        return galaxyDigest;
    }

    @JsonProperty
    public boolean isSupportGalaxyPrepare() {
        return supportGalaxyPrepare;
    }

    @JsonIgnore
    public List<ParameterContext> getFlattedParams() {
        if (flattenParams == null) {
            synchronized (this) {
                if (flattenParams == null) {
                    List<List<ParameterContext>> params = getParams();
                    flattenParams = new ArrayList<>(params.size() * (params.size() > 0 ? params.get(0).size() : 0));
                    for (List<ParameterContext> param : params) {
                        flattenParams.addAll(param);
                    }
                }
            }
        }

        return flattenParams;
    }

    @JsonProperty
    public List<List<ParameterContext>> getParams() {
        return params;
    }

    @JsonProperty
    public List<List<String>> getTableNames() {
        return tableNames;
    }

    @JsonIgnore
    public void setLimit(long limit) {
        this.limit = limit;
    }

    @JsonIgnore
    public long getLimit() {
        return limit;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("hostAddress", hostAddress)
            .add("catalogName", catalogName)
            .add("schemaName", schemaName)
            .add("dbIndex", dbIndex)
            .add("hint", new String(hint))
            .add("sqlTemplate", sqlTemplate.display())
            .add("orderBy", orderBy)
            .add("limit", limit == -1 ? null : limit)
            .add("params", params)
            .add("digest", galaxyDigest)
            .add("galaxyPrepare", supportGalaxyPrepare)
            .toString();
    }

    /**
     * clear tmp objects when query is done
     */
    public void reset() {

    }

    /**
     * for trace and record
     */
    @JsonIgnore
    public String getSqlString() {
        return new String(getHint()) + getUnionBytesSql(false).display();
    }

    public String getSqlString(boolean ignore) {
        return new String(getHint()) + getUnionBytesSql(ignore).display();
    }

    public Long getIntraGroupSortKey() {
        return intraGroupSortKey;
    }

    public void setIntraGroupSortKey(Long intraGroupSortKey) {
        this.intraGroupSortKey = intraGroupSortKey;
    }

    public Long getGrpConnId(ExecutionContext ec) {
        Boolean enableGrpParallelism = ec.getParamManager().getBoolean(ConnectionParams.ENABLE_GROUP_PARALLELISM);
        Long grpParallelism = ec.getGroupParallelism();
        return PhyTableOperationUtil.computeGrpConnIdByGrpConnKey(this.intraGroupSortKey, enableGrpParallelism,
            grpParallelism);
    }

}
