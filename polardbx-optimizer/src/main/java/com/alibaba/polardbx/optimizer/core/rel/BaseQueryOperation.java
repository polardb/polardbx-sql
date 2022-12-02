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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.Xplan.XPlanTemplate;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.protobuf.ByteString;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Map;

/**
 * @author chenmo.cm
 */
public abstract class BaseQueryOperation extends AbstractRelNode implements IPhyQueryOperation {

    protected String dbIndex;
    protected String schemaName;
    //    protected String sqlTemplate;
    protected BytesSql bytesSql;
    protected SqlNode nativeSqlNode;
    protected DbType dbType;
    protected SqlKind kind = SqlKind.SELECT;
    protected boolean useDbIndex = false; // Use by scale out write
    private int unionSize = 1;
    protected CursorMeta cursorMeta;
    protected int affectedRows = -1;

    protected XPlanTemplate XTemplate = null;
    protected ByteString sqlDigest = null;

    protected boolean supportGalaxyPrepare = true; // support by default
    protected ByteString galaxyPrepareDigest = null;

    protected boolean replicateRelNode = false;
    protected Long intraGroupConnKey = null;

    public BaseQueryOperation(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet.replace(DrdsConvention.INSTANCE));
    }

//    public BaseQueryOperation(RelOptCluster cluster, RelTraitSet traitSet, String sqlTemplate,
//                              SqlNode nativeSqlNode, DbType dbType) {
//        super(cluster, traitSet.replace(DrdsConvention.INSTANCE));
//        this.bytesSql = BytesSql.getBytesSql(sqlTemplate);
//        this.nativeSqlNode = nativeSqlNode;
//        this.dbType = dbType;
//    }

    public BaseQueryOperation(RelOptCluster cluster, RelTraitSet traitSet, BytesSql bytesSql,
                              SqlNode nativeSqlNode, DbType dbType) {
        super(cluster, traitSet.replace(DrdsConvention.INSTANCE));
        this.bytesSql = bytesSql;
        this.nativeSqlNode = nativeSqlNode;
        this.dbType = dbType;
    }

    public BaseQueryOperation(BaseQueryOperation baseQueryOperation) {
        super(baseQueryOperation.getCluster(), baseQueryOperation.getTraitSet());
        this.rowType = baseQueryOperation.rowType;
        this.dbIndex = baseQueryOperation.dbIndex;
        this.schemaName = baseQueryOperation.schemaName;
        this.bytesSql = baseQueryOperation.bytesSql;
        this.nativeSqlNode = baseQueryOperation.nativeSqlNode;
        this.dbType = baseQueryOperation.dbType;
        this.kind = baseQueryOperation.kind;
        this.unionSize = baseQueryOperation.unionSize;
        this.cursorMeta = baseQueryOperation.cursorMeta;
        this.useDbIndex = baseQueryOperation.useDbIndex;
        this.XTemplate = baseQueryOperation.XTemplate;
        this.sqlDigest = baseQueryOperation.sqlDigest;
        this.supportGalaxyPrepare = baseQueryOperation.supportGalaxyPrepare;
        this.galaxyPrepareDigest = baseQueryOperation.galaxyPrepareDigest;
    }

    @Override
    public Pair<String, Map<Integer, ParameterContext>> getDbIndexAndParam(Map<Integer, ParameterContext> param,
                                                                           List<List<String>> phyTableNamesOutput,
                                                                           ExecutionContext executionContext) {
        throw new NotSupportException();
    }

    @Override
    public String getNativeSql() {
        return bytesSql.toString(null);
    }

//    public void setSqlTemplate(String sqlTemplate) {
//        this.sqlTemplate = sqlTemplate;
//    }

    public DbType getDbType() {
        return dbType;
    }

    public void setDbType(DbType dbType) {
        this.dbType = dbType;
    }

    public SqlNode getNativeSqlNode() {
        return nativeSqlNode;
    }

    public void setNativeSqlNode(SqlNode nativeSqlNode) {
        this.nativeSqlNode = nativeSqlNode;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, getExplainName());
        pw.item("sql", this.bytesSql.display());
        return pw;
    }

    protected String getExplainName() {
        return "PhyQuery";
    }

    public SqlKind getKind() {
        return kind;
    }

    public void setKind(SqlKind kind) {
        this.kind = kind;
    }

    public void setDbIndex(String dbIndex) {
        this.dbIndex = dbIndex;
    }

    public String getDbIndex() {
        return dbIndex;
    }

    public int getUnionSize() {
        return unionSize;
    }

    public void setUnionSize(int unionSize) {
        this.unionSize = unionSize;
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public CursorMeta getCursorMeta() {
        return cursorMeta;
    }

    public boolean isUseDbIndex() {
        return useDbIndex;
    }

    public void setUseDbIndex(boolean useDbIndex) {
        this.useDbIndex = useDbIndex;
    }

    public XPlanTemplate getXTemplate() {
        return XTemplate;
    }

    public void setXTemplate(XPlanTemplate XTemplate) {
        this.XTemplate = XTemplate;
    }

    public ByteString getSqlDigest() {
        return sqlDigest;
    }

    public void setSqlDigest(ByteString sqlDigest) {
        this.sqlDigest = sqlDigest;
    }

    public boolean isSupportGalaxyPrepare() {
        return supportGalaxyPrepare;
    }

    public void setSupportGalaxyPrepare(boolean supportGalaxyPrepare) {
        this.supportGalaxyPrepare = supportGalaxyPrepare;
    }

    public ByteString getGalaxyPrepareDigest() {
        return galaxyPrepareDigest;
    }

    public void setGalaxyPrepareDigest(ByteString galaxyPrepareDigest) {
        this.galaxyPrepareDigest = galaxyPrepareDigest;
    }

    public int getAffectedRows() {
        return affectedRows;
    }

    public void setAffectedRows(int affectedRows) {
        this.affectedRows = affectedRows;
    }

    public boolean isReplicateRelNode() {
        return replicateRelNode;
    }

    public void setReplicateRelNode(boolean replicateRelNode) {
        this.replicateRelNode = replicateRelNode;
    }

    public BytesSql getBytesSql() {
        if (bytesSql == null) {
            if (nativeSqlNode != null) {
                bytesSql = RelUtils.toNativeBytesSql(nativeSqlNode, DbType.MYSQL);
            }
        }
        return bytesSql;
    }

    public void setBytesSql(BytesSql bytesSql) {
        this.bytesSql = bytesSql;
    }

    public Long getIntraGroupConnKey() {
        return intraGroupConnKey;
    }

    public void setIntraGroupConnKey(Long intraGroupConnKey) {
        this.intraGroupConnKey = intraGroupConnKey;
    }
}
