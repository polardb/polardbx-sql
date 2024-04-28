package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.optimizer.config.schema.DefaultDbSchema;
import groovy.sql.Sql;
import org.apache.calcite.rel.ddl.ConvertAllSequences;
import org.apache.calcite.sql.SqlConvertAllSequences;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class LogicalConvertAllSequences extends BaseDdlOperation {
    protected SqlConvertAllSequences sqlConvertAllSequences;

    public LogicalConvertAllSequences(ConvertAllSequences convertAllSequences) {
        super(convertAllSequences.getCluster(), convertAllSequences.getTraitSet(), convertAllSequences);
        this.sqlConvertAllSequences = (SqlConvertAllSequences) convertAllSequences.sqlNode;
        this.setSchemaName(DefaultDbSchema.NAME);
        this.relDdl.setTableName(new SqlIdentifier("*", SqlParserPos.ZERO));
        this.setTableName("*");
    }

    public static LogicalConvertAllSequences create(ConvertAllSequences convertAllSequences) {
        return new LogicalConvertAllSequences(convertAllSequences);
    }
}
