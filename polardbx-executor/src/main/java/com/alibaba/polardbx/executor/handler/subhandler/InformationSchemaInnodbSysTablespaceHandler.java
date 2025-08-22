package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaInnodbSysTablespaces;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaTablesHandler.generateMetadataForSchema;

/**
 * @author wumu
 */
public class InformationSchemaInnodbSysTablespaceHandler extends BaseVirtualViewSubClassHandler {
    private static final Logger logger = LoggerFactory.getLogger(InformationSchemaInnodbSysTablespaceHandler.class);

    private static final String DISABLE_INFO_SCHEMA_CACHE_80 = "set information_schema_stats_expiry = 0";

    private static final String QUERY_INFO_SCHEMA_INNODB_SYS_TABLESPACES =
        "select * from information_schema.innodb_sys_tablespaces where name in ('%s')";

    public InformationSchemaInnodbSysTablespaceHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaInnodbSysTablespaces;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        if (ConfigDataMode.isFastMock() || InstanceVersion.isMYSQL80()) {
            return cursor;
        }

        final int nameIndex = InformationSchemaInnodbSysTablespaces.getNameIndex();
        Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();

        // name
        Set<String> names = virtualView.getEqualsFilterValues(nameIndex, params);
        // name like
        String tableLike = virtualView.getLikeString(nameIndex, params);

        if (tableLike != null) {
            throw new TddlNestableRuntimeException("current view does not support like");
        }

        if (CollectionUtils.isEmpty(names)) {
            throw new TddlNestableRuntimeException("the name field must be specified");
        }

        Map<String, Set<String>> logicalSchemaTableName = new HashMap<>();
        for (String name : names) {
            String[] res = name.split("/");
            if (res.length != 2) {
                throw new TddlNestableRuntimeException("the name field must be '{schema_name}/{table_name}'");
            }
            logicalSchemaTableName.computeIfAbsent(res[0], k -> new HashSet<>());
            logicalSchemaTableName.get(res[0]).add(res[1]);
        }

        for (Map.Entry<String, Set<String>> entry : logicalSchemaTableName.entrySet()) {
            String schemaName = entry.getKey();
            Set<String> tableNames = entry.getValue();

            if (!OptimizerContext.getActiveSchemaNames().contains(schemaName)) {
                continue;
            }

            Map<String, Result> resultMap = new HashMap<>();
            Map<String, String> gsiToLogicalTb = new HashMap<>();
            Map<String, InformationSchemaTablesHandler.DnInfo> dnInfos =
                generateMetadataForSchema(schemaName, tableNames, null, gsiToLogicalTb);

            for (InformationSchemaTablesHandler.DnInfo dnInfo : dnInfos.values()) {
                try (Connection connection = dnInfo.getConnection();
                    Statement stmt = connection.createStatement()) {

                    Map<String, String> physicalNameToLogicalNameMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                    dnInfo.phyDbPhyTbToLogicalTb.forEach((k, v) -> {
                        physicalNameToLogicalNameMap.put(
                            rewriteToFileName(k.getKey()) + "/" + rewriteToFileName(k.getValue()), v);
                    });

                    String dbAndTb = String.join("','", physicalNameToLogicalNameMap.keySet());
                    String sql = String.format(QUERY_INFO_SCHEMA_INNODB_SYS_TABLESPACES, dbAndTb);
                    ResultSet rs = stmt.executeQuery(sql);
                    while (rs.next()) {
                        String name = rs.getString("NAME");
                        // find logical table
                        String logicalTable = physicalNameToLogicalNameMap.get(name);
                        if (null == logicalTable) {
                            logger.warn("Cant find logical table for " + name + " in " + dnInfo.dnAddress);
                            continue;
                        }
                        String logicalName = String.format("%s/%s", schemaName, logicalTable);

                        int space = rs.getInt("SPACE");
                        int flag = rs.getInt("FLAG");
                        String fileFormat = rs.getString("FILE_FORMAT");
                        String rowFormat = rs.getString("ROW_FORMAT");
                        int pageSize = rs.getInt("PAGE_SIZE");
                        int zipPageSize = rs.getInt("ZIP_PAGE_SIZE");
                        String spaceType = rs.getString("SPACE_TYPE");
                        int fsBlockSize = rs.getInt("FS_BLOCK_SIZE");
                        long fileSize = rs.getLong("FILE_SIZE");
                        long allocatedSize = rs.getLong("ALLOCATED_SIZE");

                        Result res = resultMap.computeIfAbsent(logicalName, k -> {
                            Result result = new Result();
                            result.space = space;
                            result.name = logicalName;
                            result.flag = flag;
                            result.fileFormat = fileFormat;
                            result.rowFormat = rowFormat;
                            result.pageSize = pageSize;
                            result.zipPageSize = zipPageSize;
                            result.spaceType = spaceType;
                            result.fsBlockSize = fsBlockSize;
                            return result;
                        });
                        res.fileSize += fileSize;
                        res.allocatedSize += allocatedSize;
                    }

                } catch (SQLException e) {
                    logger.error("error when querying information_schema.innodb_sys_tablespaces", e);
                    throw new TddlNestableRuntimeException(
                        "error when querying information_schema.innodb_sys_tablespaces");
                }
            }

            for (Map.Entry<String, Result> resEntry : resultMap.entrySet()) {
                Result result = resEntry.getValue();
                cursor.addRow(new Object[] {
                    result.space,
                    result.name,
                    result.flag,
                    result.fileFormat,
                    result.rowFormat,
                    result.pageSize,
                    result.zipPageSize,
                    result.spaceType,
                    result.fsBlockSize,
                    result.fileSize,
                    result.allocatedSize
                });
            }
        }

        return cursor;
    }

    private String rewriteToFileName(String name) {
        if (name == null) {
            return name;
        }

        if (name.matches("[a-zA-Z0-9_]+")) {
            return name;
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < name.length(); ++i) {
            char ch = name.charAt(i);
            if (Character.isLetterOrDigit(ch) || ch == '_') {
                sb.append(ch);
            } else {
                sb.append(String.format("@%04X", (int) ch));
            }
        }

        return sb.toString();
    }

    private static class Result {
        public int space;
        public String name;
        public int flag;
        public String fileFormat;
        public String rowFormat;
        public int pageSize;
        public int zipPageSize;
        public String spaceType;
        public int fsBlockSize;
        public long fileSize = 0;
        public long allocatedSize = 0;
    }
}
