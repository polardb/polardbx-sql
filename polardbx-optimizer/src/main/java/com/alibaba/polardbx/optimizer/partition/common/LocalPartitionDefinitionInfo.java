/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.partition.common;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLInterval;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLPartition;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByRange;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionValue;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import com.alibaba.polardbx.gms.partition.TableLocalPartitionRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.hint.HintPlanner;
import com.alibaba.polardbx.optimizer.parse.custruct.FastSqlConstructUtils;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.partition.pruning.ExprEvaluator;
import com.alibaba.polardbx.optimizer.utils.ExprContextProvider;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.google.common.base.Preconditions;
import lombok.Data;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlPartitionByRange;
import org.apache.commons.lang3.StringUtils;

import java.util.Date;
import java.util.List;
import java.util.Optional;

import static com.alibaba.polardbx.common.ddl.newengine.DdlLocalPartitionConstants.MAXVALUE;
import static com.alibaba.polardbx.common.ddl.newengine.DdlLocalPartitionConstants.MAX_LOCAL_PARTITION_COUNT;
import static com.alibaba.polardbx.common.ddl.newengine.DdlLocalPartitionConstants.PMAX;
import static com.alibaba.polardbx.common.ddl.newengine.DdlLocalPartitionConstants.VALID_PIVOT_DATE_METHOD_DATE_ADD;
import static com.alibaba.polardbx.common.ddl.newengine.DdlLocalPartitionConstants.VALID_PIVOT_DATE_METHOD_DATE_SUB;
import static com.alibaba.polardbx.common.ddl.newengine.DdlLocalPartitionConstants.VALID_PIVOT_DATE_METHOD_NOW;

@Data
public class LocalPartitionDefinitionInfo {

    public Long id;
    public Date createTime;
    public Date updateTime;
    public String tableSchema;
    public String tableName;
    public String columnName;
    public int intervalCount;
    public String intervalUnit;
    public int expireAfterCount;
    public int preAllocateCount;
    public MysqlDateTime startWithDate;
    public String pivotDateExpr;

    public String archiveTableSchema;
    public String archiveTableName;

    public boolean disableSchedule;

    public LocalPartitionDefinitionInfo() {
    }

    public MySQLIntervalType getIntervalType() {
        return MySQLIntervalType.of(intervalUnit);
    }

    public MySQLInterval getPartitionInterval() {
        MySQLIntervalType intervalType = MySQLIntervalType.of(intervalUnit);
        MySQLInterval partitionInterval = MySQLIntervalType.parseInterval(String.valueOf(intervalCount), intervalType);
        return partitionInterval;
    }

    public MySQLInterval getPreAllocateInterval() {
        return MySQLIntervalType.parseInterval(String.valueOf(preAllocateCount * intervalCount), getIntervalType());
    }

    public Optional<MySQLInterval> getExpirationInterval() {
        if (expireAfterCount < 0) {
            return Optional.empty();
        }
        return Optional.of(
            MySQLIntervalType.parseInterval(String.valueOf(expireAfterCount * intervalCount), getIntervalType()));
    }

    public static MySQLInterval getPartitionIntervalByCount(int count, int intervalCount,
                                                            MySQLIntervalType intervalType) {
        return MySQLIntervalType.parseInterval(String.valueOf(count * intervalCount), intervalType);
    }

    public static LocalPartitionDefinitionInfo from(TableLocalPartitionRecord source) {
        if (source == null) {
            return null;
        }
        LocalPartitionDefinitionInfo localPartitionInfo = new LocalPartitionDefinitionInfo();
        localPartitionInfo.setId(source.getId());
        localPartitionInfo.setCreateTime(source.getCreateTime());
        localPartitionInfo.setUpdateTime(source.getUpdateTime());
        localPartitionInfo.setTableSchema(source.getTableSchema());
        localPartitionInfo.setColumnName(source.getColumnName());
        localPartitionInfo.setTableName(source.getTableName());
        localPartitionInfo.setIntervalCount(source.getIntervalCount());
        localPartitionInfo.setIntervalUnit(source.getIntervalUnit());
        localPartitionInfo.setExpireAfterCount(source.getExpireAfterCount());
        localPartitionInfo.setPreAllocateCount(source.getPreAllocateCount());
        localPartitionInfo.setPivotDateExpr(source.getPivotDateExpr());
        localPartitionInfo.setArchiveTableName(source.getArchiveTableName());
        localPartitionInfo.setArchiveTableSchema(source.getArchiveTableSchema());

        return localPartitionInfo;
    }

    public TableLocalPartitionRecord convertToRecord() {
        TableLocalPartitionRecord localPartitionRecord = new TableLocalPartitionRecord();
        localPartitionRecord.setId(getId());
        localPartitionRecord.setCreateTime(getCreateTime());
        localPartitionRecord.setUpdateTime(getUpdateTime());
        localPartitionRecord.setTableSchema(getTableSchema());
        localPartitionRecord.setColumnName(getColumnName());
        localPartitionRecord.setTableName(getTableName());
        localPartitionRecord.setIntervalCount(getIntervalCount());
        localPartitionRecord.setIntervalUnit(getIntervalUnit());
        localPartitionRecord.setExpireAfterCount(getExpireAfterCount());
        localPartitionRecord.setPreAllocateCount(getPreAllocateCount());
        localPartitionRecord.setPivotDateExpr(getPivotDateExpr());
        localPartitionRecord.setArchiveTableSchema(getArchiveTableSchema());
        localPartitionRecord.setArchiveTableName(getArchiveTableName());
        return localPartitionRecord;
    }

    public static LocalPartitionDefinitionInfo create(String tableSchema,
                                                      String tableName,
                                                      SqlPartitionByRange localPartition) {
        Preconditions.checkNotNull(tableSchema);
        Preconditions.checkNotNull(tableName);
        if (localPartition == null) {
            return null;
        }
        SqlPartitionByRange sqlPartitionByRange = localPartition;
        SqlIdentifier column = (SqlIdentifier) sqlPartitionByRange.getColumns().get(0);
        SqlNumericLiteral intervalNum = sqlPartitionByRange.getIntervalNum();
        SqlIntervalQualifier intervalUnit = sqlPartitionByRange.getIntervalQualifier();
        SqlCharStringLiteral startWith = sqlPartitionByRange.getStartWith();
        SqlNumericLiteral expireAfter = sqlPartitionByRange.getExpireAfter();
        SqlNumericLiteral preAllocate = sqlPartitionByRange.getPreAllocate();
        SqlNode pivotDateExpr = sqlPartitionByRange.getPivotDateExpr();
        boolean disableSchedule = sqlPartitionByRange.isDisableSchedule();

        String columnName = column.getLastName();
        Integer intervalCount = intervalNum.getValueAs(Integer.class);
        String intervalUnitString = intervalUnit.toString();
        String startWithString = startWith == null ? null : startWith.toValue();
        Integer expireAfterCount = expireAfter.getValueAs(Integer.class);
        Integer preAllocateCount = preAllocate.getValueAs(Integer.class);

        LocalPartitionDefinitionInfo localPartitionInfo = new LocalPartitionDefinitionInfo();
        localPartitionInfo.setTableSchema(tableSchema);
        localPartitionInfo.setTableName(tableName);
        localPartitionInfo.setColumnName(columnName);
        localPartitionInfo.setIntervalCount(intervalCount);
        localPartitionInfo.setIntervalUnit(intervalUnitString);
        localPartitionInfo.setExpireAfterCount(expireAfterCount);
        localPartitionInfo.setPreAllocateCount(preAllocateCount);
        localPartitionInfo.setDisableSchedule(disableSchedule);
        if (StringUtils.isNotEmpty(startWithString)) {
            MysqlDateTime startWithDate = StringTimeParser.parseDatetime(startWithString.getBytes());
            localPartitionInfo.setStartWithDate(startWithDate);
        }
        localPartitionInfo.setPivotDateExpr(pivotDateExpr.toString());
        return localPartitionInfo;
    }

    public MysqlDateTime evalPivotDate(ExecutionContext executionContext) {
        executionContext = executionContext.copy();
        return evalPivotDate(pivotDateExpr, executionContext);
    }

    private MysqlDateTime evalPivotDate(String expr, ExecutionContext executionContext) {
        MySqlExprParser parser = new MySqlExprParser(ByteString.from(expr));
        SQLExpr sqlStatement = parser.expr();
        ContextParameters context = new ContextParameters(false);
        SqlNode sqlNode = FastSqlConstructUtils.convertToSqlNode(sqlStatement, context, executionContext);

        HintPlanner planner = HintPlanner.getInstance(executionContext.getSchemaName(), executionContext);
        RexNode rexNode = planner.convertExpression(sqlNode);

        ExprContextProvider holder = new ExprContextProvider();
        IExpression exprCalc = RexUtils.getEvalFuncExec(rexNode, holder);
        ExprEvaluator ee = new ExprEvaluator(holder, exprCalc);
        Object value = ee.eval(executionContext);
        return DataTypeUtil.toMySQLDatetime(value);
    }

    /**
     * valid format:
     * NOW()
     * DATE_ADD(NOW(), interval 1 month)
     * DATE_SUB(NOW(), interval 3 month)
     */
    public static void assertValidPivotDateExpr(SQLExpr pivotDateExpr) {
        if (pivotDateExpr == null) {
            throw new ParserException("syntax error. invalid pivot date expression: null");
        }
        if (!(pivotDateExpr instanceof SQLMethodInvokeExpr)) {
            throw new ParserException("syntax error. pivot date expression must be one of now/date_add/date_sub");
        }
        SQLMethodInvokeExpr expr = (SQLMethodInvokeExpr) pivotDateExpr;
        String methodName = expr.getMethodName();
        if (!StringUtils.equalsAnyIgnoreCase(methodName,
            VALID_PIVOT_DATE_METHOD_NOW, VALID_PIVOT_DATE_METHOD_DATE_ADD, VALID_PIVOT_DATE_METHOD_DATE_SUB)) {
            throw new ParserException("syntax error. pivot date expression must be one of now/date_add/date_sub");
        }
        if (StringUtils.equalsIgnoreCase(methodName, VALID_PIVOT_DATE_METHOD_NOW)) {
            return;
        }

        List<SQLExpr> args = expr.getArguments();
        if (args.size() != 2) {
            throw new ParserException("syntax error. date_add/date_sub must have 2 arguments");
        }
        SQLExpr nowExpr = args.get(0);
        if (!(nowExpr instanceof SQLMethodInvokeExpr)) {
            throw new ParserException("syntax error. pivot date expression must contain NOW() expression");
        }
        if (!StringUtils.equalsIgnoreCase(((SQLMethodInvokeExpr) nowExpr).getMethodName(),
            VALID_PIVOT_DATE_METHOD_NOW)) {
            throw new ParserException("syntax error. pivot date expression must contain NOW() expression");
        }
    }

    public static SQLPartitionByRange generateLocalPartitionStmtForCreate(
        final LocalPartitionDefinitionInfo definitionInfo,
        final MysqlDateTime initialPivotDate) {

        MysqlDateTime startWithDate = definitionInfo.getStartWithDate();
        if (startWithDate == null) {
            startWithDate = initialPivotDate;
        }
        MysqlDateTime endPartitionDate =
            MySQLTimeCalculator.addInterval(initialPivotDate, definitionInfo.getIntervalType(),
                definitionInfo.getPreAllocateInterval());

        SQLPartitionByRange partitionByRange = generatePreAllocateLocalPartitionStmt(
            definitionInfo.getColumnName(),
            startWithDate,
            endPartitionDate,
            definitionInfo.getIntervalType(),
            definitionInfo.getExpirationInterval(),
            definitionInfo.getIntervalCount()
        );

        /**
         * 限制：startWithDate + expireAfterCount * (intervalCount * intervalUnit) > pivotDate
         * 如果不允许直接创建失效的分区，则需要打开以下注释
         */
//        MysqlDateTime firstExpireDate =
//            MySQLTimeCalculator.addInterval(startWithDate, definitionInfo.getIntervalType(), definitionInfo.getExpirationInterval());
//        if(before(firstExpireDate, initialPivotDate)){
//            throw new ParserException("Not allowed to create expired local partition");
//        }

        return partitionByRange;
    }

    public static Optional<SQLPartitionByRange> generatePreAllocateLocalPartitionStmt(
        final LocalPartitionDefinitionInfo definitionInfo,
        final MysqlDateTime newestPartitionDate,
        final MysqlDateTime pivotDate) {
        //2. 跟metadb中的local partition比较，生成计划
        MysqlDateTime nextPartitionDate = MySQLTimeCalculator.addInterval(
            newestPartitionDate, definitionInfo.getIntervalType(), definitionInfo.getPartitionInterval());
        MysqlDateTime endPartitionDate = MySQLTimeCalculator.addInterval(pivotDate, definitionInfo.getIntervalType(),
            definitionInfo.getPreAllocateInterval());
        if (before(endPartitionDate, nextPartitionDate)) {
            return Optional.empty();
        }
        SQLPartitionByRange partitionByRange = generatePreAllocateLocalPartitionStmt(
            definitionInfo.getColumnName(),
            nextPartitionDate,
            endPartitionDate,
            definitionInfo.getIntervalType(),
            definitionInfo.getExpirationInterval(),
            definitionInfo.getIntervalCount()
        );
        return Optional.of(partitionByRange);
    }

    private static SQLPartitionByRange generatePreAllocateLocalPartitionStmt(String columnName,
                                                                             MysqlDateTime startWithDate,
                                                                             MysqlDateTime endPartitionDate,
                                                                             MySQLIntervalType intervalType,
                                                                             Optional<MySQLInterval> expirationInterval,
                                                                             int intervalCount) {
        SQLPartitionByRange partitionByRange = new SQLPartitionByRange();
        SQLIdentifierExpr columnExpr = new SQLIdentifierExpr(columnName);
        partitionByRange.addColumn(columnExpr);
        partitionByRange.setColumns(true);

        MysqlDateTime partitionDate = startWithDate;
        int partitionCount = 0;
        while (before(partitionDate, endPartitionDate)) {
            String date = partitionDate.toDateString();
            String pname = "p" + date.replace("-", "");
            String commentStr = genCommentStr(partitionDate, intervalType, expirationInterval);
            //generate single partition stmt
            SQLPartition partition = generateSinglePartition(pname, date, commentStr);
            partitionByRange.addPartition(partition);

            if (++partitionCount > MAX_LOCAL_PARTITION_COUNT) {
                throw new TddlNestableRuntimeException(
                    "Creating table with too many local partitions. Max is: " + MAX_LOCAL_PARTITION_COUNT);
            }
            MySQLInterval partitionIntervalAddition =
                getPartitionIntervalByCount(partitionCount, intervalCount, intervalType);
            //must add by startWithDate, otherwise may lose precision
            partitionDate = MySQLTimeCalculator.addInterval(startWithDate, intervalType, partitionIntervalAddition);
        }
        partitionByRange.addPartition(generateSinglePartition(PMAX, MAXVALUE, ""));

        return partitionByRange;
    }

    public static SQLPartition generateSinglePartition(String name, String date, String commentStr) {
        SQLPartition partition = new SQLPartition();
        SQLPartitionValue partitionValue = new SQLPartitionValue(SQLPartitionValue.Operator.LessThan);
        if (StringUtils.equalsIgnoreCase(date, MAXVALUE)) {
            partitionValue.addItem(new SQLIdentifierExpr(MAXVALUE));
        } else {
            partitionValue.addItem(new SQLCharExpr(date));
        }
        partition.setName(new SQLIdentifierExpr(name));
        partition.setValues(partitionValue);
        SQLCharExpr comment = new SQLCharExpr(commentStr);
        partition.setComment(comment);
        return partition;
    }

    public static String genCommentStr(MysqlDateTime partitionDate,
                                       MySQLIntervalType mySQLIntervalType,
                                       Optional<MySQLInterval> expireInterval) {
        return "";
    }

    public static String genExpireDateStr(String partitionDateStr,
                                          MySQLIntervalType mySQLIntervalType,
                                          Optional<MySQLInterval> expireInterval) {
        if (!expireInterval.isPresent()) {
            return "";
        }
        if (StringUtils.isEmpty(partitionDateStr) || StringUtils.equalsIgnoreCase(partitionDateStr, MAXVALUE)) {
            return "";
        }
        try {
            MysqlDateTime partitionDate = parsePartitionDate(partitionDateStr.replace("'", ""));
            MysqlDateTime expireDate =
                MySQLTimeCalculator.addInterval(partitionDate, mySQLIntervalType, expireInterval.get());
            return "expire after " + expireDate.toDatetimeString(0);
        } catch (Throwable t) {
            return "";
        }
    }

    public static MysqlDateTime parsePartitionDate(String originDateStr) {
        String dateStr = originDateStr.replace("'", "");
        MysqlDateTime partitionDate = StringTimeParser.parseDatetime(dateStr.getBytes());
        return partitionDate;
    }

    public static boolean before(MysqlDateTime date1, MysqlDateTime date2) {
        return TimeStorage.writeTimestamp(date1) <= TimeStorage.writeTimestamp(date2);
    }

    @Override
    public String toString() {
        return String.format(
            "LOCAL PARTITION BY RANGE (%s)\n"
                + "INTERVAL %s %s\n"
                + "EXPIRE AFTER %s\n"
                + "PRE ALLOCATE %s\n"
                + "PIVOTDATE %s\n",
            columnName,
            intervalCount,
            intervalUnit,
            expireAfterCount,
            preAllocateCount,
            pivotDateExpr
        );
    }
}