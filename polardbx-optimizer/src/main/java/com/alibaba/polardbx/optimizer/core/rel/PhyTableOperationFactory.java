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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;

/**
 * @author chenghui.lch
 */
public class PhyTableOperationFactory extends PhyOperationBuilderCommon {

    private static final PhyTableOperationFactory instance = new PhyTableOperationFactory();

    private PhyTableOperationFactory() {
        super();
    }

    public static PhyTableOperationFactory getInstance() {
        return instance;
    }

    /**
     * Build a PhyTableOperation with full properties that can be executed directly
     * by using build params
     */
    public PhyTableOperation buildPhyTblOpByParams(PhyTableOpBuildParams buildParams) {
        return buildPhyTblOpByParamsInner(buildParams, true);
    }

    /**
     * Build a PhyTableOperation of select without any tables
     * with full properties that can be executed directly
     * by using build params
     */
    public PhyTableOperation buildPhyTblOpByParamsForSelectWithoutTable(PhyTableOpBuildParams buildParams) {
        return buildPhyTblOpByParamsInner(buildParams, false);
    }

    /**
     * Build a PhyTableOperation with full properties that can be executed directly
     * by using another phyTableOp template and new build params
     */
    public PhyTableOperation buildPhyTableOperationByPhyOp(PhyTableOperation targetPhyOp,
                                                           PhyTableOpBuildParams newBuildParams) {
        return buildPhyTableOperationByPhyOpInner(targetPhyOp, newBuildParams, true);
    }

    /**
     * Build a PhyTableOperation template with non-full properties that can NOT be executed directly
     */
    public PhyTableOperation buildPhyTblOpTemplate(PhyTableOpBuildParams opTemplateBuildParams) {
        PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
        buildParams.setSchemaName(opTemplateBuildParams.schemaName);
        buildParams.setLogTables(opTemplateBuildParams.logTables);
        buildParams.setGroupName(opTemplateBuildParams.groupName);
        buildParams.setPhyTables(opTemplateBuildParams.phyTables);
        buildParams.setSqlKind(opTemplateBuildParams.sqlKind);
        if (opTemplateBuildParams.lockMode != null) {
            buildParams.setLockMode(opTemplateBuildParams.lockMode);
        }
        buildParams.setOnlyOnePartitionAfterPruning(opTemplateBuildParams.onlyOnePartitionAfterPruning);

        buildParams.setLogicalPlan(opTemplateBuildParams.logicalPlan);
        buildParams.setCluster(opTemplateBuildParams.cluster);
        buildParams.setTraitSet(opTemplateBuildParams.traitSet);
        buildParams.setRowType(opTemplateBuildParams.rowType);
        buildParams.setCursorMeta(opTemplateBuildParams.cursorMeta);

        buildParams.setBytesSql(opTemplateBuildParams.bytesSql);
        buildParams.setDbType(DbType.MYSQL);
        buildParams.setDynamicParams(opTemplateBuildParams.dynamicParams);
        buildParams.setParamIndexMapping(opTemplateBuildParams.paramIndexMapping);
        buildParams.setBatchParameters(opTemplateBuildParams.batchParameters);

        PhyTableOperation operation = buildPhyTblOpByParamsInner(buildParams, false);
        return operation;
    }

    /**
     * Only used by PhyTableOperation.copy
     */
    public PhyTableOperation copyFrom(PhyTableOperation targetPhyOp) {
        return buildPhyTableOperationByPhyOpInner(targetPhyOp, null, false);
    }

    private PhyTableOperation buildPhyTableOperationByPhyOpInner(PhyTableOperation targetPhyOp,
                                                                 PhyTableOpBuildParams newBuildParams,
                                                                 boolean needCheck) {

        PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
        buildParams.setSchemaName(targetPhyOp.getSchemaName());
        buildParams.setLogTables(targetPhyOp.getLogicalTableNames());
        buildParams.setGroupName(targetPhyOp.getDbIndex());
        buildParams.setPhyTables(targetPhyOp.getTableNames());
        buildParams.setSqlKind(targetPhyOp.getKind());
        buildParams.setLockMode(targetPhyOp.getLockMode());
        buildParams.setOnlyOnePartitionAfterPruning(targetPhyOp.isOnlyOnePartitionAfterPruning());

        buildParams.setLogicalPlan(targetPhyOp.getParent());
        buildParams.setCluster(targetPhyOp.getCluster());
        buildParams.setTraitSet(targetPhyOp.getTraitSet());
        buildParams.setRowType(targetPhyOp.getRowType());
        buildParams.setCursorMeta(targetPhyOp.getCursorMeta());
        buildParams.setNativeSqlNode(targetPhyOp.getNativeSqlNode());

        buildParams.setxTemplate(targetPhyOp.getXTemplate());
        buildParams.setSqlDigest(targetPhyOp.getSqlDigest());
        buildParams.setSupportGalaxyPrepare(targetPhyOp.isSupportGalaxyPrepare());
        buildParams.setGalaxyPrepareDigest(targetPhyOp.getGalaxyPrepareDigest());
        buildParams.setMemoryAllocator(targetPhyOp.getMemoryAllocator());
        buildParams.setBuilderCommon(targetPhyOp.getPhyOperationBuilder());
        buildParams.setUnionSize(targetPhyOp.getUnionSize());

        buildParams.setBytesSql(targetPhyOp.getBytesSql());
        buildParams.setDbType(targetPhyOp.getDbType());
        buildParams.setDynamicParams(targetPhyOp.getParam());
        buildParams.setParamIndexMapping(targetPhyOp.getParamIndex());
        buildParams.setBatchParameters(targetPhyOp.getBatchParameters());

        if (newBuildParams != null) {

            if (newBuildParams.schemaName != null) {
                buildParams.setSchemaName(newBuildParams.schemaName);
            }

            if (newBuildParams.groupName != null) {
                buildParams.setGroupName(newBuildParams.groupName);
            }

            if (newBuildParams.logTables != null && !newBuildParams.logTables.isEmpty()) {
                buildParams.setLogTables(newBuildParams.logTables);
            }

            if (newBuildParams.phyTables != null && !newBuildParams.phyTables.isEmpty()) {
                buildParams.setPhyTables(newBuildParams.phyTables);
            }

            if (newBuildParams.dynamicParams != null) {
                buildParams.setDynamicParams(newBuildParams.dynamicParams);
            }

            if (newBuildParams.paramIndexMapping != null) {
                buildParams.setParamIndexMapping(newBuildParams.paramIndexMapping);
            }

            if (newBuildParams.bytesSql != null) {
                buildParams.setBytesSql(newBuildParams.bytesSql);
            }

            if (newBuildParams.batchParameters != null) {
                buildParams.setBatchParameters(newBuildParams.batchParameters);
            }

            if (newBuildParams.lockMode != null) {
                buildParams.setLockMode(newBuildParams.lockMode);
            }

            /**
             *  Because of performance of building phyOp,
             *  here just allowed to replace the above properties of PhyTableOpBuildParams,
             *  you can add more "setXXX" to support replacing more properties of PhyTableOpBuildParams
             */
        }
        PhyTableOperation operation = buildPhyTblOpByParamsInner(buildParams, needCheck);
        return operation;
    }

    private PhyTableOperation buildPhyTblOpByParamsInner(PhyTableOpBuildParams buildParams, boolean needCheck) {

        PhyTableOperation operation = new PhyTableOperation(
            buildParams.cluster,
            buildParams.traitSet,
            buildParams.rowType,
            buildParams.cursorMeta,
            buildParams.logicalPlan);

        operation.setSchemaName(buildParams.schemaName);
        operation.setLogicalTableNames(buildParams.logTables);
        operation.setDbIndex(buildParams.groupName);
        operation.setTableNames(buildParams.phyTables);
        operation.setKind(buildParams.sqlKind);
        if (buildParams.lockMode != null) {
            operation.setLockMode(buildParams.lockMode);
        }
        operation.setOnlyOnePartitionAfterPruning(buildParams.onlyOnePartitionAfterPruning);

        operation.setBytesSql(buildParams.bytesSql);
        operation.setDbType(DbType.MYSQL);
        operation.setParam(buildParams.dynamicParams);
        operation.setParamIndex(buildParams.paramIndexMapping);
        operation.setBatchParameters(buildParams.batchParameters);

        if (buildParams.nativeSqlNode != null) {
            operation.setNativeSqlNode(buildParams.nativeSqlNode);
        }
        operation.setXTemplate(buildParams.xTemplate);
        operation.setSqlDigest(buildParams.sqlDigest);
        operation.setSupportGalaxyPrepare(buildParams.supportGalaxyPrepare);
        operation.setGalaxyPrepareDigest(buildParams.galaxyPrepareDigest);
        operation.setUnionSize(buildParams.unionSize);
        operation.setPhyOperationBuilder(buildParams.builderCommon);

        if (needCheck) {
            if (!checkIfPhyTableOperationValid(operation)) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    String.format("Found invalid physical table operation during executing plan"));
            }
        }

        return operation;
    }

    private boolean checkIfPhyTableOperationValid(PhyTableOperation phyOb) {
        if (phyOb.getSchemaName() == null) {
            return false;
        }

        if (DbInfoManager.getInstance().isNewPartitionDb(phyOb.getSchemaName())) {
            if (phyOb.getLogicalTableNames() == null || phyOb.getLogicalTableNames().isEmpty()) {
                return false;
            }

            if (StringUtils.isEmpty(phyOb.getDbIndex())) {
                return false;
            }

            if (phyOb.getTableNames() == null || phyOb.getTableNames().isEmpty()) {
                return false;
            }
        }
        return true;
    }
}
