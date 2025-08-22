package com.alibaba.polardbx.server.handler;

import com.alibaba.polardbx.common.ColumnarOptions;
import com.alibaba.polardbx.common.columnar.ColumnarOption;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ColumnarConfig;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.table.ColumnarConfigAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarConfigRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author yaozhili
 */
public class ColumnarConfigHandler {
    private static final Logger logger = LoggerFactory.getLogger(ColumnarConfigHandler.class);
    /**
     * Define columnar config properties here.
     */
    private static final Map<String, ColumnarOption> CONFIG =
        new ImmutableMap.Builder<String, ColumnarOption>()

            .put(ColumnarOptions.DICTIONARY_COLUMNS, new ColumnarOption(
                ColumnarOptions.DICTIONARY_COLUMNS, null,
                "Columnar index dictionary columns.",
                null, null, null,
                ColumnarOption.CaseSensitive.UPPERCASE_KEY
            ))

            .put(ColumnarOptions.TYPE, new ColumnarOption(
                ColumnarOptions.TYPE, "default",
                "Columnar index type, choices: default, snapshot.",
                ColumnarConfigHandler::setIndexType,
                null,
                ColumnarConfigHandler::validateType,
                ColumnarOption.CaseSensitive.UPPERCASE_KEY_UPPERCASE_VALUE
            ))

            .put(ColumnarOptions.SNAPSHOT_RETENTION_DAYS, new ColumnarOption(
                ColumnarOptions.SNAPSHOT_RETENTION_DAYS, "7",
                "Columnar snapshot retention days, default is 7 days.",
                ColumnarConfigHandler::setParamAndUpdateMeta,
                ColumnarConfigHandler::setColumnarConfig,
                ColumnarConfigHandler::validateSnapshotRetentionDays,
                ColumnarOption.CaseSensitive.UPPERCASE_KEY_UPPERCASE_VALUE
            ))

            .put(ColumnarOptions.AUTO_GEN_COLUMNAR_SNAPSHOT_INTERVAL, new ColumnarOption(
                ColumnarOptions.AUTO_GEN_COLUMNAR_SNAPSHOT_INTERVAL, "-1",
                "Columnar snapshot retention auto generated interval (in minutes), default is -1 (not auto-generated).",
                ColumnarConfigHandler::setParamAndUpdateMeta,
                ColumnarConfigHandler::setColumnarConfig,
                ColumnarConfigHandler::validateAutoGenColumnarSnapshotInterval,
                ColumnarOption.CaseSensitive.UPPERCASE_KEY_UPPERCASE_VALUE
            ))

            .put(ColumnarOptions.COLUMNAR_HEARTBEAT_INTERVAL_MS_SELF_ADAPTION, new ColumnarOption(
                ColumnarOptions.COLUMNAR_HEARTBEAT_INTERVAL_MS_SELF_ADAPTION, "true",
                "Columnar heartbeat interval self adaption, default: true.",
                null,
                ColumnarConfigHandler::setColumnarConfig,
                ColumnarConfigHandler::validateBoolean,
                ColumnarOption.CaseSensitive.LOWERCASE_KEY
            ))

            .put(ColumnarOptions.COLUMNAR_BACKUP_ENABLE, new ColumnarOption(
                ColumnarOptions.COLUMNAR_BACKUP_ENABLE, "false",
                "Whether enable columnar backup for this columnar index.",
                null,
                ColumnarConfigHandler::setColumnarConfig,
                ColumnarConfigHandler::validateBoolean,
                ColumnarOption.CaseSensitive.LOWERCASE_KEY
            ))

            .build();

    private static void setParamAndUpdateMeta(ColumnarOption.Param param) {
        setColumnarConfig(param);
    }

    private static void setIndexType(ColumnarOption.Param param) {
        try {
            generateSchemaAndTableByTableId(param);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "Failed to get schema and table name.");
        }
        // cci name -> config key -> val
        Map<String, Map<String, String>> records = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        Map<String, String> globalConfig = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        MetaDbUtil.generateColumnarConfig(param.schemaName, param.tableName, records, globalConfig);
        Map<String, String> indexConfig = records.get(param.indexName);

        String currentType = null == indexConfig ? ColumnarConfig.DEFAULT : indexConfig.get(ColumnarOptions.TYPE);
        try (Connection connection = MetaDbUtil.getConnection()) {
            try {
                connection.setAutoCommit(false);
                if (!param.value.equalsIgnoreCase(currentType)) {
                    if (ColumnarConfig.DEFAULT.equalsIgnoreCase(currentType)) {
                        // default -> snapshot
                        Preconditions.checkArgument(ColumnarConfig.SNAPSHOT.equalsIgnoreCase(param.value));
                        // If SNAPSHOT_RETENTION_DAYS not set, set a default one.
                        if (null == indexConfig || !indexConfig.containsKey(
                            ColumnarOptions.SNAPSHOT_RETENTION_DAYS)) {
                            ColumnarOption.Param tmpParam = param.shallowCopy();
                            tmpParam.key = ColumnarOptions.SNAPSHOT_RETENTION_DAYS;
                            tmpParam.value = ColumnarConfig.getValue(tmpParam.key, null, globalConfig);
                            String columnarPurgeSaveMs = globalConfig.get(ColumnarOptions.COLUMNAR_PURGE_SAVE_MS);
                            if (null != columnarPurgeSaveMs) {
                                long columnarPurgeSaveDays =
                                    1 + Long.parseLong(columnarPurgeSaveMs) / 1000 / 60 / 60 / 24;
                                if (columnarPurgeSaveDays > Long.parseLong(tmpParam.value)) {
                                    tmpParam.value = Long.toString(columnarPurgeSaveDays);
                                }
                            }
                            setColumnarConfig(tmpParam, connection);
                        }
                        // If AUTO_GEN_COLUMNAR_SNAPSHOT_INTERVAL not set, set a default one.
                        if (null == indexConfig || !indexConfig.containsKey(
                            ColumnarOptions.AUTO_GEN_COLUMNAR_SNAPSHOT_INTERVAL)) {
                            ColumnarOption.Param tmpParam = param.shallowCopy();
                            tmpParam.key = ColumnarOptions.AUTO_GEN_COLUMNAR_SNAPSHOT_INTERVAL;
                            tmpParam.value = ColumnarConfig.getValue(tmpParam.key, null, globalConfig);
                            setColumnarConfig(tmpParam, connection);
                        }
                        // Force backup.
                        {
                            ColumnarOption.Param tmpParam = param.shallowCopy();
                            tmpParam.key = ColumnarOptions.COLUMNAR_BACKUP_ENABLE;
                            tmpParam.value = "true";
                            tmpParam.caseSensitive = ColumnarOption.CaseSensitive.LOWERCASE_KEY;
                            setColumnarConfig(tmpParam, connection);
                        }
                        ColumnarTableMappingAccessor accessor = new ColumnarTableMappingAccessor();
                        accessor.setConnection(connection);
                        accessor.updateTypeByTableId(param.tableId, ColumnarConfig.SNAPSHOT);
                        accessor.UpdateExtraByTableId(param.tableId, null);
                    } else {
                        // snapshot -> default
                        Preconditions.checkArgument(ColumnarConfig.SNAPSHOT.equalsIgnoreCase(currentType));
                        Preconditions.checkArgument(ColumnarConfig.DEFAULT.equalsIgnoreCase(param.value));
                        if (null != indexConfig && indexConfig.containsKey(
                            ColumnarOptions.SNAPSHOT_RETENTION_DAYS)) {
                            ColumnarOption.Param tmpParam = param.shallowCopy();
                            tmpParam.key = ColumnarOptions.SNAPSHOT_RETENTION_DAYS;
                            deleteColumnarConfig(tmpParam, connection);
                        }
                        if (null != indexConfig && indexConfig.containsKey(
                            ColumnarOptions.AUTO_GEN_COLUMNAR_SNAPSHOT_INTERVAL)) {
                            ColumnarOption.Param tmpParam = param.shallowCopy();
                            tmpParam.key = ColumnarOptions.AUTO_GEN_COLUMNAR_SNAPSHOT_INTERVAL;
                            deleteColumnarConfig(tmpParam, connection);
                        }
                        ColumnarTableMappingAccessor accessor = new ColumnarTableMappingAccessor();
                        accessor.setConnection(connection);
                        accessor.updateTypeByTableId(param.tableId, null);
                    }
                    setColumnarConfig(param, connection);
                    connection.commit();
                }
            } catch (Throwable t) {
                connection.rollback();
                connection.setAutoCommit(true);
                throw t;
            }
        } catch (SQLException e) {
            logger.error(e);
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG,
                "Set columnar config failed, caused by " + e.getMessage());
        }
    }

    private static void validateType(ColumnarOption.Param param) {
        if (!isOneOfString(param.value, ColumnarConfig.DEFAULT, ColumnarConfig.SNAPSHOT)) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "Invalid type " + param.value
                + ", choices: " + ColumnarConfig.DEFAULT + ", " + ColumnarConfig.SNAPSHOT);
        }
        param.value = param.value.toUpperCase();
    }

    private static void validateSnapshotRetentionDays(ColumnarOption.Param param) {
        long days = Long.parseLong(param.value);
        if (days < 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "Value should larger than 0.");
        }
        param.value = Long.toString(days);
    }

    private static void validateAutoGenColumnarSnapshotInterval(ColumnarOption.Param param) {
        long minutes = Long.parseLong(param.value);
        if (minutes >= 0 && minutes <= 5) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "Value too small, should >= 5");
        }
        param.value = Long.toString(minutes);
    }

    private static void validateBoolean(ColumnarOption.Param param) {
        param.value = String.valueOf(parseBoolean(param.value));
    }

    private static boolean parseBoolean(String v) {
        String stripVal = StringUtils.strip(v, "'\"");
        if (stripVal == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "Value should be boolean.");
        }
        if ("true".equalsIgnoreCase(stripVal)
            || "on".equalsIgnoreCase(stripVal)
            || "1".equalsIgnoreCase(stripVal)) {
            return true;
        }
        if ("false".equalsIgnoreCase(stripVal)
            || "off".equalsIgnoreCase(stripVal)
            || "0".equalsIgnoreCase(stripVal)) {
            return false;
        }
        throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "Value should be boolean.");
    }

    private static boolean isOneOfString(Object val, String... args) {
        if (val == null) {
            return false;
        }
        for (String s : args) {
            if (s.equalsIgnoreCase(val.toString())) {
                return true;
            }
        }
        return false;
    }

    private static void generateSchemaAndTableByTableId(ColumnarOption.Param param) throws SQLException {
        if (null == param.schemaName) {
            try (Connection connection = MetaDbUtil.getConnection()) {
                ColumnarTableMappingAccessor columnarTableMappingAccessor = new ColumnarTableMappingAccessor();
                columnarTableMappingAccessor.setConnection(connection);
                List<ColumnarTableMappingRecord> records = columnarTableMappingAccessor.queryTableId(param.tableId);
                if (records.isEmpty()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_CONFIG,
                        "Update table version failed: empty columnar records.");
                }
                param.schemaName = records.get(0).tableSchema;
                param.tableName = records.get(0).tableName;
                param.indexName = records.get(0).indexName;
            }
        }
    }

    /**
     * Record config in system table columnar_config.
     */
    public static void setColumnarConfig(ColumnarOption.Param param) {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            setColumnarConfig(param, metaDbConn);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void setColumnarConfig(ColumnarOption.Param param, Connection connection) {
        ColumnarConfigAccessor accessor = new ColumnarConfigAccessor();
        accessor.setConnection(connection);
        ColumnarConfigRecord record = new ColumnarConfigRecord();
        switch (param.caseSensitive) {
        case LOWERCASE_KEY:
            record.configKey = param.key.toLowerCase();
            record.configValue = param.value;
            break;
        case UPPERCASE_KEY:
            record.configKey = param.key.toUpperCase();
            record.configValue = param.value;
            break;
        case LOWERCASE_KEY_LOWERCASE_VALUE:
            record.configKey = param.key.toLowerCase();
            record.configValue = param.value.toLowerCase();
            break;
        case UPPERCASE_KEY_UPPERCASE_VALUE:
            record.configKey = param.key.toUpperCase();
            record.configValue = param.value.toUpperCase();
            break;
        case DEFAULT:
        default:
            record.configKey = param.key;
            record.configValue = param.value;
            break;
        }
        param.key = record.configKey;
        param.value = record.configValue;
        record.tableId = param.tableId;
        accessor.insert(ImmutableList.of(record));
    }

    public static void deleteColumnarConfig(ColumnarOption.Param param, Connection connection) {
        ColumnarConfigAccessor accessor = new ColumnarConfigAccessor();
        accessor.setConnection(connection);
        ColumnarConfigRecord record = new ColumnarConfigRecord();
        record.configKey = param.key.toUpperCase();
        record.tableId = param.tableId;
        accessor.deleteByTableIdAndKey(ImmutableList.of(record));
    }

    static {
        ColumnarConfig.init(CONFIG);
    }

    public static void init() {
    }
}
