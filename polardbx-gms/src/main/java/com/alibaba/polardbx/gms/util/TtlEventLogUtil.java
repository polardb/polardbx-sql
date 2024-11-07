package com.alibaba.polardbx.gms.util;

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

/**
 * @author chenghui.lch
 */
public class TtlEventLogUtil {

    // TTL 任务日志
    private final static Logger TTL_TASK_LOGGER = LoggerFactory.getLogger("ttl_task");

    public static void logCreateTtlDefinitionEvent(String schemaName,
                                                   String ttlTblName) {
        try {
            String msg = String.format("new ttl-definition of table[`%s`.`%s`] has been created",
                schemaName, ttlTblName);
            EventLogger.log(EventType.CREATE_TTL_DEFINITION, msg);

        } catch (Throwable ex) {
            TTL_TASK_LOGGER.error(ex);
        }
    }

    public static void logCreateTtlDefinitionWithArchiveTableEvent(String schemaName,
                                                                   String ttlTblName,
                                                                   String archiveTable) {
        try {
            String msg =
                String.format("new ttl-definition of table[`%s`.`%s`] with archive table[`%s`.`%s`] has been created",
                    schemaName, ttlTblName, schemaName, archiveTable);
            EventLogger.log(EventType.CREATE_TTL_DEFINITION_WITH_ARCHIVE_TABLE, msg);

        } catch (Throwable ex) {
            TTL_TASK_LOGGER.error(ex);
        }
    }

    public static void logCreateCciArchiveTableEvent(String schemaName,
                                                     String ttlTableName,
                                                     String archiveTable) {
        try {
            String msg =
                String.format("new cci-based archive table[`%s`.`%s`] of ttl table[`%s`.`%s`] has been created",
                    schemaName, archiveTable, schemaName, ttlTableName);
            EventLogger.log(EventType.CREATE_CCI_ARCHIVE_TABLE, msg);
        } catch (Throwable ex) {
            TTL_TASK_LOGGER.error(ex);
        }
    }

    public static void logDropCciArchiveTableEvent(String schemaName,
                                                   String ttlTableName,
                                                   String archiveTable) {
        try {
            String msg =
                String.format("the cci-based archive table[`%s`.`%s`] of ttl table[`%s`.`%s`] has been dropped",
                    schemaName, archiveTable, schemaName, ttlTableName);
            EventLogger.log(EventType.DROP_CCI_ARCHIVE_TABLE, msg);
        } catch (Throwable ex) {
            TTL_TASK_LOGGER.error(ex);
        }
    }

    public static void logCleanupExpiredDataEvent(String schemaName,
                                                  String ttlTableName) {
        try {
            String msg = String.format("the ttl table[`%s`.`%s`] has cleanup the expired data",
                schemaName, ttlTableName);
            EventLogger.log(EventType.CLEANUP_EXPIRED_DATA, msg);
        } catch (Throwable ex) {
            TTL_TASK_LOGGER.error(ex);
        }
    }
}
