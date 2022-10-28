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
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.Xplan.XPlanTemplate;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.google.protobuf.ByteString;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;

import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class PhyTableOpBuildParams {
    /**
     * ======= Properties used by fetch group connid, which are NOT allowed to be NULL=======
     */
    /**
     * The schema name of PhyTableOperation to be build
     * Notice: a PhyTableOperation is NOT allowed to access cross multi schemas
     */
    protected String schemaName;
    /**
     * The group name of PhyTableOperation to be build
     * Notice: a PhyTableOperation is NOT allowed to access cross multi groups
     */
    protected String groupName;
    /**
     * The list of logical table names of  PhyTableOperation to be access
     * <pre>
     *     1. For example of Query with join pushed down:
     *     if the logical query  "select * from hash_tbl a join broadcast_tbl b on a.id=b.id" is pushed down,
     *     Then the list of logical table names is
     *          [hash_tbl, broadcast_tbl] , which positions are the same as they exist in original sql
     *
     *     2. For example of Query without join pushed down:
     *     if the logical query  "select * from hash_tbl a" is pushed down,
     *     Then the list of logical table names is [hash_tbl]
     *
     *     3. For example of single-table update/delete:
     *     if the logical dml  "update hash_tbl set a=xx" is pushed down,
     *     Then the list of logical table names is the source table [hash_tbl]
     *
     *     4. For example of insert select:
     *     if the logical insert  "insert into hash_tbl3 select * from hash_tbl join hash_tbl2" is pushed down,
     *     (hash_tbl1,hash_tbl2 and hash_tbl3 are in the same table_group )
     *     Then the list of logical table names is target table and all source tables [hash_tbl3,hash_tbl1,hash_tbl2],
     *     which positions are the same as they exist in original sql
     *
     *     5. For example of multi-table update:
     *     if the logical dml  "update hash_tbl as t1,hash_tbl2 as t2 set t1.b=1, t2.b=1 where t1.a=t2.a;" is pushed down,
     *     then the list of logical table names is source tables [hash_tbl,hash_tbl2]
     *
     *     6. For example of multi-table delete:
     *     if the logical dml  "delete t1,t2 from hash_tbl as t1 inner join hash_tbl2 as t2 where t1.a=t2.a;" is pushed down,
     *     then the list of logical table names is source tables [hash_tbl,hash_tbl2]
     * </pre>
     */
    protected List<String> logTables;

    /**
     * The list of physical tables list that are the same partition group of PhyTableOperation to be access
     * <pre>
     *
     *  1. For example of Query with join pushed down with partition tbls:
     *     if the logical query  "select * from hash_tbl1 a join hash_tbl2 b on a.id=b.id" is pushed down,
     *     so the list of logical tables is
     *          [hash_tbl1, hash_tbl2] , which positions are the same as they exist in original sql
     *
     *     then the the list of physical table list ( List<List> ) is
     *          [
     *              [hash_tbl1_00, hash_tbl2_00]
     *              [hash_tbl1_01, hash_tbl2_01]
     *              [hash_tbl1_02, hash_tbl2_02]
     *              ...
     *          ]
     *
     *  2. For example of Query with join pushed down with broadcast tbl:
     *     if the logical query  "select * from hash_tbl a join broadcast_tbl b on a.id=b.id" is pushed down,
     *     so the list of logical tables is
     *          [hash_tbl, broadcast_tbl] , which positions are the same as they exist in original sql
     *
     *     then the the list of physical table list ( List<List> ) is
     *          [
     *              [hash_tbl_00, broadcast_tbl]
     *              [hash_tbl_01, broadcast_tbl]
     *              [hash_tbl_02, broadcast_tbl]
     *              ...
     *          ]
     *
     * </pre>
     */
    protected List<List<String>> phyTables;
    /**
     * Lable the sql kind of physical sql, which will tell TransConnectionHolder to fetch write connections or not
     */
    protected SqlKind sqlKind = SqlKind.SELECT;
    /**
     * Label if the physical sql of query is using any lock like " FOR UPDATE" and so on.
     * , The default value of lockMode of PhyTableOperation is UNDEF
     */
    protected SqlSelect.LockMode lockMode;

    /**
     * Label if there is only one PhyTableOperation and go to pushdown by PostPlanner after pruning LogicalView
     */
    protected boolean onlyOnePartitionAfterPruning = false;

    /**
     * =======Used by X-protocol only=======
     */
    protected XPlanTemplate xTemplate;
    protected ByteString sqlDigest;

    /**
     * =======Used by GalaxyPrepare only=======
     */
    protected boolean supportGalaxyPrepare = false; // disable it by default(most case in backfill etc.)
    protected ByteString galaxyPrepareDigest = null;

    /**
     * =======Used by calc the allocated memory of all PhyTableOperation only=======
     */
    protected MemoryAllocatorCtx memoryAllocator;
    protected PhyOperationBuilderCommon builderCommon;
    protected int unionSize = 1;

    /**
     * =======Used to build a normal PhyTableOperation or PhyTableOperation template=======
     */
    protected RelOptCluster cluster;
    protected RelTraitSet traitSet;
    protected RelDataType rowType;
    protected CursorMeta cursorMeta;
    protected RelNode logicalPlan;
    protected DbType dbType = DbType.MYSQL;
    protected SqlNode nativeSqlNode;

    protected BytesSql bytesSql;
    protected Map<Integer, ParameterContext> dynamicParams;
    protected List<Map<Integer, ParameterContext>> batchParameters;

    public PhyTableOpBuildParams() {
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public void setLogTables(List<String> logTables) {
        this.logTables = logTables;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public void setPhyTables(List<List<String>> phyTables) {
        this.phyTables = phyTables;
    }

    public void setSqlKind(SqlKind sqlKind) {
        this.sqlKind = sqlKind;
    }

    public void setCluster(RelOptCluster cluster) {
        this.cluster = cluster;
    }

    public void setTraitSet(RelTraitSet traitSet) {
        this.traitSet = traitSet;
    }

    public void setRowType(RelDataType rowType) {
        this.rowType = rowType;
    }

    public void setCursorMeta(CursorMeta cursorMeta) {
        this.cursorMeta = cursorMeta;
    }

    public void setLogicalPlan(RelNode logicalPlan) {
        this.logicalPlan = logicalPlan;
    }

    public void setBytesSql(BytesSql bytesSql) {
        this.bytesSql = bytesSql;
    }

    public void setDbType(DbType dbType) {
        this.dbType = dbType;
    }

    public void setDynamicParams(
        Map<Integer, ParameterContext> dynamicParams) {
        this.dynamicParams = dynamicParams;
    }

    public void setBatchParameters(
        List<Map<Integer, ParameterContext>> batchParameters) {
        this.batchParameters = batchParameters;
    }

    public void setLockMode(SqlSelect.LockMode lockMode) {
        this.lockMode = lockMode;
    }

    public void setxTemplate(XPlanTemplate xTemplate) {
        this.xTemplate = xTemplate;
    }

    public void setSqlDigest(ByteString sqlDigest) {
        this.sqlDigest = sqlDigest;
    }

    public void setSupportGalaxyPrepare(boolean supportGalaxyPrepare) {
        this.supportGalaxyPrepare = supportGalaxyPrepare;
    }

    public void setGalaxyPrepareDigest(ByteString galaxyPrepareDigest) {
        this.galaxyPrepareDigest = galaxyPrepareDigest;
    }

    public void setMemoryAllocator(MemoryAllocatorCtx memoryAllocator) {
        this.memoryAllocator = memoryAllocator;
    }

    public void setBuilderCommon(PhyOperationBuilderCommon builderCommon) {
        this.builderCommon = builderCommon;
    }

    public void setUnionSize(int unionSize) {
        this.unionSize = unionSize;
    }

    public void setNativeSqlNode(SqlNode nativeSqlNode) {
        this.nativeSqlNode = nativeSqlNode;
    }

    public void setOnlyOnePartitionAfterPruning(boolean onlyOnePartitionAfterPruning) {
        this.onlyOnePartitionAfterPruning = onlyOnePartitionAfterPruning;
    }
}
