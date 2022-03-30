package com.alibaba.polardbx.cdc;

import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;

/**
 * Sql Utils for Cdc moudle
 * Created by ziyang.lb
 **/
public class SQLHelper {
    public final static SQLParserFeature[] FEATURES = {
        SQLParserFeature.EnableSQLBinaryOpExprGroup,
        SQLParserFeature.UseInsertColumnsCache, SQLParserFeature.OptimizedForParameterized,
        SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL,
        SQLParserFeature.DRDSBaseline, SQLParserFeature.DrdsMisc, SQLParserFeature.DrdsGSI, SQLParserFeature.DrdsCCL
    };
}
