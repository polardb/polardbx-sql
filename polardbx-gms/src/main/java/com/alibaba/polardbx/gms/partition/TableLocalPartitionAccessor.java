package com.alibaba.polardbx.gms.partition;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.TABLE_LOCAL_PARTITIONS;

public class TableLocalPartitionAccessor extends AbstractAccessor {

    private static final Logger logger = LoggerFactory.getLogger(TableLocalPartitionAccessor.class);

    private static final String SYS_TABLE_NAME = TABLE_LOCAL_PARTITIONS;

    private static final String ALL_COLUMNS =
        "`id`,`create_time`,`update_time`,`table_schema`,"
            + "`table_name`,`column_name`,`interval_count`,"
            + "`interval_unit`,`expire_after_count`,`pre_allocate_count`,"
            + "`pivot_date_expr`";

    private static final String ALL_VALUES = "(null,now(),now(),?,?,?,?,?,?,?,?)";

    private static final String INSERT_TABLE_LOCAL_PARTITIONS =
        "insert into table_local_partitions (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES;

    private static final String GET_TABLE_LOCAL_PARTITIONS =
        "select " + ALL_COLUMNS
            + " from table_local_partitions";

    private static final String GET_TABLE_LOCAL_PARTITIONS_BY_SCHEMA =
        "select " + ALL_COLUMNS
            + " from table_local_partitions where table_schema=? order by table_name";

    private static final String GET_TABLE_LOCAL_PARTITIONS_BY_SCHEMA_TABLE =
        "select " + ALL_COLUMNS
            + " from table_local_partitions where table_schema=? and table_name=?";

    private static final String DELETE_SQL =
        "delete from " + SYS_TABLE_NAME + " where table_schema=? and table_name=?";

    private static final String DELETE_ALL_SQL =
        "delete from " + SYS_TABLE_NAME + " where table_schema=? ";

    public int insert(TableLocalPartitionRecord record) {
        try {
            DdlMetaLogUtil.logSql(INSERT_TABLE_LOCAL_PARTITIONS, record.buildParams());
            return MetaDbUtil.insert(INSERT_TABLE_LOCAL_PARTITIONS, record.buildParams(), connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to insert a new record into table_local_partitions", "insert into", e);
        }
    }

    public List<TableLocalPartitionRecord> query() {
        try {
            DdlMetaLogUtil.logSql(GET_TABLE_LOCAL_PARTITIONS);
            return MetaDbUtil.query(GET_TABLE_LOCAL_PARTITIONS, TableLocalPartitionRecord.class, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to query table_local_partitions", "select", e);
        }
    }

    public List<TableLocalPartitionRecord> queryBySchema(String schemaName) {
        return query(
            GET_TABLE_LOCAL_PARTITIONS_BY_SCHEMA,
            SYS_TABLE_NAME,
            TableLocalPartitionRecord.class,
            schemaName
        );
    }

    public TableLocalPartitionRecord queryByTableName(String schemaName, String tableName) {
        List<TableLocalPartitionRecord> list = query(
            GET_TABLE_LOCAL_PARTITIONS_BY_SCHEMA_TABLE,
            SYS_TABLE_NAME,
            TableLocalPartitionRecord.class,
            schemaName,
            tableName
        );
        return CollectionUtils.isNotEmpty(list)? list.get(0): null;
    }

    public int delete(String schemaName, String tableName){
        return delete(DELETE_SQL, SYS_TABLE_NAME, schemaName, tableName);
    }

    public int deleteAll(String schemaName){
        return delete(DELETE_ALL_SQL, SYS_TABLE_NAME, schemaName);
    }


    private TddlRuntimeException logAndThrow(String errMsg, String action, Exception e) {
        logger.error(errMsg, e);
        return new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, action,
            "table_local_partitions", e.getMessage());
    }

}