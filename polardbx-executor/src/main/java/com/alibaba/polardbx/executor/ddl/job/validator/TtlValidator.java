package com.alibaba.polardbx.executor.ddl.job.validator;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlUtil;

/**
 * @author chenghui.lch
 */
public class TtlValidator {
    public static void validateIfDroppingCciOfArcTableOfTtlTable(String tableSchema,
                                                                 String tableName,
                                                                 String indexName,
                                                                 ExecutionContext ec) {

        if (!TtlUtil.checkIfDropCciOfArcTblView(tableSchema, tableName, indexName, ec)) {
            return;
        }

        /**
         * Come here, that means the cci still bound to the ttl tbl by ttl_info.archive_tmp_table_name, so should throw error
         */
        throw new TddlRuntimeException(ErrorCode.ERR_TTL,
            String.format("Dropping a columnar index `%s` of archive data of table `%s`.`%s` is not allowed.",
                indexName, tableSchema, tableName));
    }

    public static void validateIfAllowPerformRepartition(String tableSchema,
                                                         String tableName,
                                                         PartitionInfo newPartInfo,
                                                         ExecutionContext ec) {
        if (!DbInfoManager.getInstance().isNewPartitionDb(tableSchema)) {
            return;
        }
        TableMeta tableMeta = ec.getSchemaManager(tableSchema).getTable(tableName);
        TtlDefinitionInfo ttlInfo = tableMeta.getTtlDefinitionInfo();
        if (ttlInfo == null) {
            return;
        }
        boolean isChangeToBroTblOrSigTbl =
            newPartInfo.isGsiSingleOrSingleTable() || newPartInfo.isGsiBroadcastOrBroadcast();
        if (!isChangeToBroTblOrSigTbl) {
            return;
        }
        /**
         * Come here, that means the part_tbl with ttl-def will be changed to single/broadcast
         */
        throw new TddlRuntimeException(ErrorCode.ERR_TTL,
            String.format("Repartition `%s`.`%s` to single/broadcast with ttl-definition is not allowed", tableSchema,
                tableName));
    }
}
