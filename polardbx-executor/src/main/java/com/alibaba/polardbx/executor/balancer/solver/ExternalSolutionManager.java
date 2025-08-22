package com.alibaba.polardbx.executor.balancer.solver;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.backfill.BackfillSampleManager;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.optimizer.config.table.GsiUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.gms.util.MetaDbUtil.query;

public class ExternalSolutionManager {
    static DataSource dataSource = MetaDbDataSource.getInstance().getDataSource();

    private static final String SQL_QUERY_SOLUTION =
        "SELECT id, schema_name, storage_pool_name, table_group_name, solution, invisible, extra FROM  "
            + GmsSystemTables.REBALANCE_EXTERNAL_SOLUTION + " WHERE "
            + " schema_name = ? and storage_pool_name = ? and table_group_name = ? order by id desc ";

    private static final Long INVISIBLE = 1L;

    public static final String EMPTY_SOLUTION = "EMPTY_SOLUTION";

    public interface Orm<T> {
        T convert(ResultSet resultSet) throws SQLException;

        Map<Integer, ParameterContext> params();
    }

    public static final class ExternalSolutionRecord implements
        ExternalSolutionManager.Orm<ExternalSolutionManager.ExternalSolutionRecord> {
        public static ExternalSolutionRecord ORM = new ExternalSolutionRecord();

        Long id;
        String schemaName;
        String storagePoolName;
        String tableGroupName;
        String solution;
        Long invisible;
        String extra;

        ExternalSolutionRecord() {
        }

        ExternalSolutionRecord(Long id, String schemaName, String storagePoolName, String tableGroupName,
                               String solution, Long invisible,
                               String extra) {
            this.id = id;
            this.schemaName = schemaName;
            this.storagePoolName = storagePoolName;
            this.tableGroupName = tableGroupName;
            this.solution = solution;
            this.invisible = invisible;
            this.extra = extra;
        }

        @Override
        public ExternalSolutionRecord convert(ResultSet resultSet) throws SQLException {
            final Long id = resultSet.getLong("id");
            final String schemaName = resultSet.getString("SCHEMA_NAME");
            final String storagePoolName = resultSet.getString("STORAGE_POOL_NAME");
            final String tableGroupName = resultSet.getString("TABLE_GROUP_NAME");
            final String solution = resultSet.getString("SOLUTION");
            final Long invisible = resultSet.getLong("INVISIBLE");
            final String extra = resultSet.getString("EXTRA");

            return new ExternalSolutionRecord(
                id,
                schemaName,
                storagePoolName,
                tableGroupName,
                solution,
                invisible,
                extra);
        }

        @Override
        public Map<Integer, ParameterContext> params() {
            return Collections.emptyMap();
        }
    }

    public static String queryExternalSolution(String schemaName, String storagePoolName, String tableGroupName) {
        final List<ExternalSolutionRecord> externalSolutionRecords = new ArrayList<>();

        Map<Integer, ParameterContext> params = new HashMap<>();
        params.put(1, new ParameterContext(ParameterMethod.setString, new Object[] {1, schemaName}));
        params.put(2, new ParameterContext(ParameterMethod.setString, new Object[] {2, storagePoolName}));
        params.put(3, new ParameterContext(ParameterMethod.setString, new Object[] {3, tableGroupName}));

        GsiUtils.wrapWithTransaction(dataSource, (conn) -> {
                try {
                    externalSolutionRecords.addAll(
                        query(SQL_QUERY_SOLUTION, params, ExternalSolutionRecord.ORM, conn));
                } catch (SQLException e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_REBALANCE,
                        e,
                        "query solution failed!");
                }
            }, (e) -> new TddlRuntimeException(ErrorCode.ERR_REBALANCE, e,
                "query solution failed!")
        );
        String solution = null;
        String extra = null;
        Long invisible = null;
        if (!externalSolutionRecords.isEmpty()) {
            solution = externalSolutionRecords.get(0).solution;
            extra = externalSolutionRecords.get(0).extra;
            invisible = externalSolutionRecords.get(0).invisible;
        }
        if (StringUtils.equalsIgnoreCase(extra, "INVISIBLE") || INVISIBLE.equals(invisible)) {
            return EMPTY_SOLUTION;
        } else {
            return solution;
        }
    }

    private static <T> List<T> query(String sqlQuerySolution, Map<Integer, ParameterContext> params,
                                     ExternalSolutionManager.Orm<T> orm, Connection connection)
        throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(sqlQuerySolution)) {
            ParameterMethod.setParameters(ps, params);

            final ResultSet rs = ps.executeQuery();

            final List<T> result = new ArrayList<>();
            while (rs.next()) {
                result.add(orm.convert(rs));
            }
            return result;
        }
    }
}
