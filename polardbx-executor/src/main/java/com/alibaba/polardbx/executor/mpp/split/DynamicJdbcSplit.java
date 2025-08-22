package com.alibaba.polardbx.executor.mpp.split;

import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.UnionBytesSql;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DynamicJdbcSplit extends JdbcSplit {

    /**
     * 对应同个split下的多张分表，如果某一项为 null 则表示不需要该分表
     */
    private List<SqlNode> lookupConditions;

    public DynamicJdbcSplit(JdbcSplit jdbcSplit, SqlNode lookupCondition) {
        this(jdbcSplit, Collections.nCopies(jdbcSplit.getTableNames().size(), lookupCondition));
        this.supportGalaxyPrepare = false;
    }

    public DynamicJdbcSplit(JdbcSplit jdbcSplit, List<SqlNode> lookupConditions) {
        super(jdbcSplit);
        Preconditions.checkArgument(jdbcSplit.getTableNames().size() == lookupConditions.size());
        this.lookupConditions = lookupConditions;
        this.supportGalaxyPrepare = false;
    }

    @Override
    public BytesSql getUnionBytesSql(boolean ignore) {
        if (ignore) {
            int num = 0;
            for (SqlNode condition : lookupConditions) {
                // Build physical query ignoring the FALSE splits
                if (condition != null) {
                    num++;
                }
            }
            if (num == 1) {
                return sqlTemplate;
            } else {
                return new UnionBytesSql(sqlTemplate.getBytesArray(), sqlTemplate.isParameterLast(), num,
                    orderBy == null ? null : orderBy.getBytes(), limit > 0 ? (limit + "").getBytes() : null);
            }
        }
        if (hintSql == null) {
            int num = 0;
            for (SqlNode condition : lookupConditions) {
                // Build physical query ignoring the FALSE splits
                if (condition != null) {
                    num++;
                }
            }
            String query;
            if (num == 1) {
                query = sqlTemplate.toString(null);
            } else {
                query = new UnionBytesSql(sqlTemplate.getBytesArray(), sqlTemplate.isParameterLast(), num,
                    orderBy == null ? null : orderBy.getBytes(), limit > 0 ? (limit + "").getBytes() : null).toString(
                    null);
            }
            for (SqlNode condition : lookupConditions) {
                if (condition != null) {
                    String conditionSql = RelUtils.toNativeSql(condition);
                    // escape condition sql using mysql escape char '\'
                    String dialectSql = TStringUtil.escape(conditionSql, '\\', '\\');
                    query = StringUtils.replace(query, "'bka_magic' = 'bka_magic'", dialectSql, 1);
                }
            }
            hintSql = query;
        }
        return BytesSql.getBytesSql(hintSql);
    }

    @Override
    public List<ParameterContext> getFlattedParams() {
        if (flattenParams == null) {
            synchronized (this) {
                if (flattenParams == null) {
                    List<List<ParameterContext>> params = getParams();
                    flattenParams = new ArrayList<>(
                        params.size() > 0 ? params.get(0).size() * lookupConditions.size() : 0);
                    for (int i = 0; i < lookupConditions.size(); i++) {
                        final SqlNode cond = lookupConditions.get(i);
                        // Build physical parameters ignoring the FALSE splits
                        if (cond != null) {
                            flattenParams.addAll(params.get(i));
                        }
                    }
                }
            }
        }
        return flattenParams;
    }

    @Override
    public void reset() {
        this.hintSql = null;
        this.lookupConditions = null;
    }
}
