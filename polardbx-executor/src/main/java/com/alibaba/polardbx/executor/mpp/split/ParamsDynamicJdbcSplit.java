package com.alibaba.polardbx.executor.mpp.split;

import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.jdbc.UnionBytesSql;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.LookupTableSortScanExec;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * ParamsDynamicJdbcSplit is match for {@link LookupTableSortScanExec}
 */
public class ParamsDynamicJdbcSplit extends JdbcSplit {

    private static final int MAX_CACHE_SIZE = 4000;
    private static final int CACHE_EXPIRE_TIME = 12 * 3600 * 1000; // 12h

    private static Cache<CacheKey, BytesSql> cacheForUnionSql = buildCache(MAX_CACHE_SIZE);

    private static Cache<CacheKey, BytesSql> buildCache(long maxSize) {
        return CacheBuilder.newBuilder()
            .maximumSize(maxSize)
            .expireAfterAccess(CACHE_EXPIRE_TIME, TimeUnit.MILLISECONDS)
            .softValues()
            .build();
    }

    private Chunk chunk;
    private boolean inflateMode = false;
    private int unionNum = 1;
    private int originSqlUnionNum = 1;

    public ParamsDynamicJdbcSplit(JdbcSplit jdbcSplit, Chunk chunk, boolean inflateMode) {
        super(jdbcSplit);
        this.chunk = chunk;
        this.supportGalaxyPrepare = false;
        this.inflateMode = inflateMode;
        this.unionNum = inflateMode ? chunk.getPositionCount() : 1;
        this.originSqlUnionNum = params == null ? 1 : params.size();
    }

    @Override
    public BytesSql getUnionBytesSql(boolean ignore) {
        int unionAllNum = unionNum * originSqlUnionNum;
        CacheKey cacheKey = new CacheKey(
            getSchemaName(), sqlTemplate.toString(), select, orderBy, limit != -1, unionAllNum);
        BytesSql bytesSql = null;
        try {
            bytesSql = cacheForUnionSql.get(cacheKey, new Callable<BytesSql>() {
                @Override
                public BytesSql call() throws Exception {

                    BytesSql template = startSql;
                    if (startSql == null) {
                        template = sqlTemplate;
                    }

                    if (ignore) {
                        if (unionAllNum == 1) {
                            return sqlTemplate;
                        } else {
                            byte[] unionHead = UnionBytesSql.UNION_HEAD;
                            if (!StringUtils.isEmpty(select)) {
                                unionHead = ("SELECT " + select + " FROM (").getBytes();
                            }
                            return new UnionBytesSql(template.getBytesArray(), template.isParameterLast(),
                                unionAllNum,
                                orderBy == null ? null : orderBy.getBytes(), limit > 0 ? ("?").getBytes() : null,
                                unionHead);
                        }
                    }
                    if (hintSql == null) {
                        String query;
                        if (unionAllNum == 1) {
                            query = sqlTemplate.toString(null);
                        } else {
                            byte[] unionHead = UnionBytesSql.UNION_HEAD;
                            if (!StringUtils.isEmpty(select)) {
                                unionHead = ("SELECT " + select + " FROM (").getBytes();
                            }
                            query = new UnionBytesSql(template.getBytesArray(), template.isParameterLast(),
                                unionAllNum,
                                orderBy == null ? null : orderBy.getBytes(), limit > 0 ? ("?").getBytes() : null,
                                unionHead).toString(
                                null);
                        }
                        hintSql = query;
                    }
                    return BytesSql.buildBytesSql(hintSql);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return bytesSql;
    }

    @Override
    public List<ParameterContext> getFlattedParams() {
        if (flattenParams == null) {
            synchronized (this) {
                if (flattenParams == null) {
                    List<List<ParameterContext>> params = getParams();
                    if (inflateMode) {
                        flattenParams = new ArrayList<>(
                            params.size() > 0 ? params.get(0).size() * chunk.getPositionCount() : 0);
                        for (int i = 0; i < chunk.getPositionCount(); i++) {
                            Chunk.ChunkRow row = chunk.rowAt(i);
                            for (List<ParameterContext> parameterContexts : params) {
                                for (ParameterContext parameterContext : parameterContexts) {
                                    if (parameterContext.getParameterMethod() == ParameterMethod.setDelegateInValue) {
                                        List<Object> objectListList = new ArrayList<>(chunk.getBlockCount());
                                        if (chunk.getBlockCount() == 1) {
                                            objectListList.addAll(row.getJavaValues());
                                        } else {
                                            objectListList.add(row.getJavaValues());
                                        }
                                        RawString rawString = new RawString(objectListList);
                                        flattenParams.add(
                                            new ParameterContext(ParameterMethod.setObject1, new Object[] {
                                                parameterContext.getArgs()[0], rawString}));
                                    } else {
                                        flattenParams.add(parameterContext);
                                    }
                                }
                            }
                        }
                    } else {
                        flattenParams = new ArrayList<>(
                            params.size() > 0 ? params.get(0).size() : 0);
                        for (List<ParameterContext> parameterContexts : params) {
                            for (ParameterContext parameterContext : parameterContexts) {
                                if (parameterContext.getParameterMethod() == ParameterMethod.setDelegateInValue) {
                                    List<Object> objectListList = new ArrayList<>(chunk.getPositionCount());
                                    RawString rawString = new RawString(objectListList);
                                    for (int i = 0; i < chunk.getPositionCount(); i++) {
                                        Chunk.ChunkRow row = chunk.rowAt(i);
                                        if (chunk.getBlockCount() == 1) {
                                            objectListList.addAll(row.getJavaValues());
                                        } else {
                                            objectListList.add(row.getJavaValues());
                                        }
                                    }
                                    flattenParams.add(new ParameterContext(ParameterMethod.setObject1, new Object[] {
                                        parameterContext.getArgs()[0], rawString}));
                                } else {
                                    flattenParams.add(parameterContext);
                                }
                            }
                        }
                    }
                    if (unionNum * originSqlUnionNum > 1) {
                        if (limit > 0) {
                            flattenParams.add(new ParameterContext(ParameterMethod.setLong, new Object[] {0, limit}));
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
        this.chunk = null;
        this.flattenParams = null;
    }

    public static class CacheKey {
        private final String schema;
        private final String parameterizedSql;
        private final String orderBy;
        private final String select;
        private final boolean existLimit;
        private final long unionNum;

        public CacheKey(
            String schema, String parameterizedSql, String select, String orderBy, boolean existLimit, long unionNum) {
            this.schema = schema;
            this.parameterizedSql = parameterizedSql;
            this.select = select;
            this.orderBy = orderBy;
            this.existLimit = existLimit;
            this.unionNum = unionNum;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return existLimit == cacheKey.existLimit && unionNum == cacheKey.unionNum && Objects.equals(schema,
                cacheKey.schema) && Objects.equals(parameterizedSql, cacheKey.parameterizedSql)
                && Objects.equals(orderBy, cacheKey.orderBy) && Objects.equals(select, cacheKey.select);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schema, parameterizedSql, select, orderBy, existLimit, unionNum);
        }
    }
}
