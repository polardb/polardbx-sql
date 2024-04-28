package com.alibaba.polardbx.common.trx;

import com.alibaba.polardbx.common.constants.SystemTables;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.google.protobuf.ByteString;

import java.security.MessageDigest;

/**
 * @author yaozhili
 */
public class TrxLogTableConstants {
    public final static String SET_DISTRIBUTED_TRX_ID = "SET polarx_distributed_trx_id = %s";
    protected final static Logger logger = LoggerFactory.getLogger(TrxLogTableConstants.class);

    public static final String ALTER_GLOBAL_TX_TABLE_COMMIT_TS =
        "ALTER TABLE `" + SystemTables.DRDS_GLOBAL_TX_LOG + "` "
            + "ADD COLUMN `COMMIT_TS` BIGINT DEFAULT NULL, "
            + "ADD COLUMN `PARTICIPANTS` BLOB DEFAULT NULL, "
            + "ALGORITHM=INPLACE, LOCK=NONE";

    public static final String ALTER_GLOBAL_TX_TABLE_TYPE_ENUMS =
        "ALTER TABLE `" + SystemTables.DRDS_GLOBAL_TX_LOG + "` MODIFY COLUMN `TYPE` "
            + "enum('TCC', 'XA', 'BED', 'TSO', 'HLC') NOT NULL";

    public static final String GLOBAL_TX_TABLE_GET_PARTITIONS =
        "SELECT `PARTITION_NAME`, `PARTITION_DESCRIPTION`, `TABLE_ROWS` FROM  INFORMATION_SCHEMA.PARTITIONS\n"
            + "WHERE TABLE_NAME = '" + SystemTables.DRDS_GLOBAL_TX_LOG + "'\n"
            + "AND TABLE_SCHEMA = DATABASE()";

    public static final String ALTER_GLOBAL_TX_TABLE_DROP_PARTITION_PREFIX =
        "ALTER TABLE `" + SystemTables.DRDS_GLOBAL_TX_LOG + "` \n"
            + "DROP PARTITION ";

    public static final String REDO_LOG_TABLE = SystemTables.DRDS_REDO_LOG;

    public static final String ALTER_REDO_LOG_TABLE = "ALTER TABLE `" + REDO_LOG_TABLE + "` "
        + "ADD COLUMN `SCHEMA` VARCHAR(64) NULL AFTER `TXID`";

    public static final String CREATE_REDO_LOG_TABLE =
        "CREATE TABLE IF NOT EXISTS `" + REDO_LOG_TABLE + "` (\n"
            + "  `TXID` BIGINT NOT NULL,\n"
            + "  `SCHEMA` VARCHAR(64) NULL,\n"
            + "  `SEQ`  INT(11) NOT NULL,\n"
            + "  `INFO` LONGTEXT NOT NULL,\n"
            + "  PRIMARY KEY (`TXID`, `SEQ`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n";

    private static final String GLOBAL_TX_TABLE_MAX_PARTITION = "p_unlimited";

    public static final String ALTER_GLOBAL_TX_TABLE_ADD_PARTITION =
        "ALTER TABLE `" + SystemTables.DRDS_GLOBAL_TX_LOG + "` \n"
            + "REORGANIZE PARTITION `" + GLOBAL_TX_TABLE_MAX_PARTITION + "` INTO \n"
            + "(PARTITION `%s` VALUES LESS THAN (%d), PARTITION `" + GLOBAL_TX_TABLE_MAX_PARTITION
            + "` VALUES LESS THAN MAXVALUE)";

    public static final String ALTER_GLOBAL_TX_TABLE_ADD_MAX_PARTITION =
        "ALTER TABLE `" + SystemTables.DRDS_GLOBAL_TX_LOG + "` \n"
            + "PARTITION BY RANGE (`TXID`) (PARTITION `" + GLOBAL_TX_TABLE_MAX_PARTITION
            + "` VALUES LESS THAN MAXVALUE)";

    public static final String ALTER_GLOBAL_TX_TABLE_INIT_PARTITION =
        "ALTER TABLE `" + SystemTables.DRDS_GLOBAL_TX_LOG + "` \n"
            + "PARTITION BY RANGE (`TXID`) (PARTITION `" + GLOBAL_TX_TABLE_MAX_PARTITION
            + "` VALUES LESS THAN MAXVALUE)";

    public static final String CREATE_GLOBAL_TX_TABLE =
        "CREATE TABLE IF NOT EXISTS `" + SystemTables.DRDS_GLOBAL_TX_LOG + "` (\n"
            + "  `TXID` BIGINT NOT NULL,\n"
            + "  `START_TIME` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "  `TYPE` ENUM('TCC', 'XA', 'BED', 'TSO', 'HLC') NOT NULL,\n"
            + "  `STATE` ENUM('PREPARE', 'COMMIT', 'ROLLBACK', 'SUCCEED', 'ABORTED') NOT NULL,\n"
            + "  `RETRIES` INT(11) NOT NULL DEFAULT 0,\n"
            + "  `COMMIT_TS` BIGINT DEFAULT NULL,\n"
            + "  `PARTICIPANTS` BLOB DEFAULT NULL,\n"
            + "  `TIMEOUT` TIMESTAMP NULL,\n"
            + "  `SERVER_ADDR` VARCHAR(21) NOT NULL,\n"
            + "  `CONTEXT` TEXT NOT NULL,\n"
            + "  `ERROR` TEXT NULL,\n"
            + "  PRIMARY KEY (`TXID`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8\n"
            + "PARTITION BY RANGE (`TXID`) (PARTITION `%s` VALUES LESS THAN (%d), PARTITION `"
            + GLOBAL_TX_TABLE_MAX_PARTITION + "` VALUES LESS THAN MAXVALUE)";

    /**
     * Trx Log Table V2.
     * Column: TXID, COMMIT_TS, N_PARTICIPANTS
     */
    public static final String CREATE_GLOBAL_TX_TABLE_V2 =
        "CREATE TABLE IF NOT EXISTS " + SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE
            + " (\n"
            + "  `TXID` BIGINT UNSIGNED NOT NULL,\n"
            + "  `TRX_SEQ` BIGINT UNSIGNED NOT NULL DEFAULT 18446744073709551615 COMMENT \"DEFAULT INVALID_SEQUENCE_NUMBER\",\n"
            + "  `N_PARTICIPANTS` INT UNSIGNED NOT NULL DEFAULT 0,\n"
            + "  PRIMARY KEY (`TXID`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

    /**
     * V2 tmp table.
     */
    public static final String DROP_GLOBAL_TX_TABLE_V2_TMP =
        "DROP TABLE IF EXISTS " + SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE_TMP;
    public static final String CREATE_GLOBAL_TX_TABLE_V2_TMP =
        "CREATE TABLE IF NOT EXISTS " + SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE_TMP
            + " (\n"
            + "  `TXID` BIGINT UNSIGNED NOT NULL,\n"
            + "  `TRX_SEQ` BIGINT UNSIGNED NOT NULL DEFAULT 18446744073709551615 COMMENT \"DEFAULT INVALID_SEQUENCE_NUMBER\",\n"
            + "  `N_PARTICIPANTS` INT UNSIGNED NOT NULL DEFAULT 0,\n"
            + "  PRIMARY KEY (`TXID`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

    /**
     * Switch V2 table to ARCHIVE, tmp table to V2.
     */
    public static final String SWITCH_GLOBAL_TX_TABLE_V2 =
        "RENAME TABLE " + SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE + " TO "
            + SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE_ARCHIVE + ", "
            + SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE_TMP + " TO " + SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE;

    /**
     * In case unexpected situations, force renaming tmp to V2.
     */
    public static final String FORCE_RENAME_GLOBAL_TX_TABLE_V2 =
        "RENAME TABLE " + SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE_TMP
            + " TO " + SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE;

    public static final String SHOW_ALL_GLOBAL_TX_TABLE_V2 = String.format(
        "select table_name from information_schema.tables where table_schema = '%s' and table_name like '%s%%'",
        SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE_DB, SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE_PREFIX);

    public static final String SELECT_MAX_TX_ID_IN_ARCHIVE =
        "SELECT max(TXID) FROM " + SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE_ARCHIVE;

    /**
     * Drop archive table if it is not needed any more.
     */
    public static final String DROP_GLOBAL_TX_TABLE_V2_ARCHIVE =
        "DROP TABLE IF EXISTS " + SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE_ARCHIVE;

    /**
     * Note: We want to use same digest for all sql on different physical DB, so we treat table as a parameter.
     */
    public static final String APPEND_TRX =
        "INSERT INTO ? (`TXID`, `TYPE`, `STATE`, `SERVER_ADDR`, `CONTEXT`) VALUES (?, ?, ?, ?, ?)";

    public static final String APPEND_TRX_WITH_TS =
        "INSERT INTO ? (`TXID`, `TYPE`, `STATE`, `SERVER_ADDR`, `CONTEXT`, `COMMIT_TS`) VALUES (?, ?, ?, ?, ?, ?)";

    /**
     * Column: TXID, START_TIME, TYPE, STATE, COMMIT_TS, N_PARTICIPANTS, SERVER_ADDR, EXTRA
     */
    public static final String APPEND_TRX_V2 =
        "INSERT INTO " + SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE +
            " (`TXID`, `TRX_SEQ`) VALUES (?, ?)";

    public static final String DELETE_ASYNC_COMMIT_TRX =
        "DELETE FROM " + SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE +
            " WHERE `TXID` = ?";

    public static final String SELECT_BY_ID =
        "SELECT `TYPE`, `STATE`, `SERVER_ADDR`, `CONTEXT`, `COMMIT_TS` FROM ? WHERE `TXID` = ?";

    /**
     * Column: TXID, `TRX_SEQ`, `N_PARTICIPANTS`
     */
    public static final String SELECT_BY_ID_V2 =
        "SELECT `TRX_SEQ`, `N_PARTICIPANTS` FROM " + SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE + " WHERE `TXID` = %s";

    public static final String SELECT_BY_ID_V2_ARCHIVE =
        "SELECT `TRX_SEQ`, `N_PARTICIPANTS` FROM " + SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE_ARCHIVE
            + " WHERE `TXID` = %s";

    public static final String SELECT_TABLE_ROWS_V2 =
        "SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES "
            + "WHERE TABLE_SCHEMA = '" + SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE_DB + "'"
            + " AND TABLE_NAME = '" + SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE_ARCHIVE_TABLE + "'";

    public static final String EXISTS_GLOBAL_TX_TABLE_V2 =
        "SELECT 1 FROM information_schema.tables WHERE table_schema = '"
            + SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE_DB +
            "' AND table_name = '" + SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE_ARCHIVE_TABLE + "'";

    /**
     * 18446744073709551611 is a magic snapshot sequence. Using it you can see prepared trx.
     */
    public static final String RECOVER_TIMESTAMP_SQL = "SET innodb_snapshot_seq = 18446744073709551611";

    public static ByteString APPEND_TRX_DIGEST;
    public static ByteString APPEND_TRX_WITH_TS_DIGEST;
    public static ByteString SELECT_BY_ID_DIGEST;

    static {
        try {
            final MessageDigest md5 = MessageDigest.getInstance("md5");
            TrxLogTableConstants.APPEND_TRX_DIGEST =
                ByteString.copyFrom(md5.digest(TrxLogTableConstants.APPEND_TRX.getBytes()));
            TrxLogTableConstants.APPEND_TRX_WITH_TS_DIGEST =
                ByteString.copyFrom(md5.digest(TrxLogTableConstants.APPEND_TRX_WITH_TS.getBytes()));
            TrxLogTableConstants.SELECT_BY_ID_DIGEST =
                ByteString.copyFrom(md5.digest(TrxLogTableConstants.SELECT_BY_ID.getBytes()));
        } catch (Exception e) {
            logger.error(e);
        }
    }
}
