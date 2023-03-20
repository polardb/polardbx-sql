package com.alibaba.polardbx.executor.ddl.job.task.cdc;

import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;

/**
 * created by ziyang.lb
 **/
public class CdcSqlUtils {
    public final static SQLParserFeature[] SQL_PARSE_FEATURES = {
        SQLParserFeature.EnableSQLBinaryOpExprGroup,
        SQLParserFeature.UseInsertColumnsCache, SQLParserFeature.OptimizedForParameterized,
        SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL,
        SQLParserFeature.DRDSBaseline, SQLParserFeature.DrdsMisc, SQLParserFeature.DrdsGSI, SQLParserFeature.DrdsCCL
    };
}
