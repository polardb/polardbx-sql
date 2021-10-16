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

package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.ast.SQLLimit;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBooleanExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntervalExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterCharacter;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLReplaceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowColumnsStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowCreateTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowCreateViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowDatabasesStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowIndexesStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLStartTransactionStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlForceIndexHint;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlIgnoreIndexHint;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlUseIndexHint;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlCharExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlOutFileExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlUserName;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.CobarShowStatus;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableDiscardTablespace;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableImportTablespace;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableOption;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlBinlogStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement.TableSpaceOption;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateUserStatement.UserSpecification;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlExecuteStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlKillStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlLoadXmlStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlLockTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlPartitionByKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlPrepareStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlResetStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSetTransactionStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowAuthorsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowBinLogEventsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowBinaryLogsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowCharacterSetStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowCollationStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowContributorsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateDatabaseStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateEventStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateFunctionStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateProcedureStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateTriggerStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowEngineStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowEnginesStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowErrorsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowEventsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowFunctionCodeStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowFunctionStatusStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowGrantsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowMasterLogsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowMasterStatusStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowOpenTablesStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowPluginsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowPrivilegesStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowProcedureCodeStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowProcedureStatusStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowProcessListStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowProfileStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowRelayLogEventsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowSlaveHostsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowSlaveStatusStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowStatusStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowTableStatusStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowTriggersStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowWarningsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlUnlockTablesStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MysqlDeallocatePrepareStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import junit.framework.TestCase;

public class MySqlASTVisitorAdapterTest extends TestCase {

    public void test_adapter() throws Exception {
        MySqlASTVisitorAdapter adapter = new MySqlASTVisitorAdapter();
        new SQLBooleanExpr().accept(adapter);
        new SQLLimit().accept(adapter);
        new MySqlTableIndex().accept(adapter);
        new MySqlKey().accept(adapter);
        new MySqlPrimaryKey().accept(adapter);
        new SQLIntervalExpr().accept(adapter);
        new SQLBinaryExpr().accept(adapter);
        new MySqlPrepareStatement().accept(adapter);
        new MySqlExecuteStatement().accept(adapter);
        new MysqlDeallocatePrepareStatement().accept(adapter);
        new MySqlDeleteStatement().accept(adapter);
        new MySqlInsertStatement().accept(adapter);
        new MySqlLoadXmlStatement().accept(adapter);
        new SQLReplaceStatement().accept(adapter);
        new SQLStartTransactionStatement().accept(adapter);
        new SQLShowColumnsStatement().accept(adapter);
        new SQLShowDatabasesStatement().accept(adapter);
        new MySqlShowWarningsStatement().accept(adapter);
        new MySqlShowStatusStatement().accept(adapter);
        new CobarShowStatus().accept(adapter);
        new MySqlKillStatement().accept(adapter);
        new MySqlBinlogStatement().accept(adapter);
        new MySqlResetStatement().accept(adapter);
        new UserSpecification().accept(adapter);
        new MySqlPartitionByKey().accept(adapter);
        new MySqlOutFileExpr().accept(adapter);
        new MySqlUpdateStatement().accept(adapter);
        new MySqlSetTransactionStatement().accept(adapter);
        new MySqlShowMasterLogsStatement().accept(adapter);
        new MySqlShowAuthorsStatement().accept(adapter);
        new MySqlShowCollationStatement().accept(adapter);
        new MySqlShowBinLogEventsStatement().accept(adapter);
        new MySqlShowCharacterSetStatement().accept(adapter);
        new MySqlShowContributorsStatement().accept(adapter);
        new MySqlShowCreateDatabaseStatement().accept(adapter);
        new MySqlShowCreateEventStatement().accept(adapter);
        new MySqlShowCreateFunctionStatement().accept(adapter);
        new MySqlShowCreateProcedureStatement().accept(adapter);
        new SQLShowCreateTableStatement().accept(adapter);
        new MySqlShowCreateTriggerStatement().accept(adapter);
        new SQLShowCreateViewStatement().accept(adapter);
        new MySqlShowEngineStatement().accept(adapter);
        new MySqlShowEnginesStatement().accept(adapter);
        new MySqlShowErrorsStatement().accept(adapter);
        new MySqlShowEventsStatement().accept(adapter);
        new MySqlShowFunctionCodeStatement().accept(adapter);
        new MySqlShowFunctionStatusStatement().accept(adapter);
        new MySqlShowGrantsStatement().accept(adapter);
        new MySqlUserName().accept(adapter);
        new SQLShowIndexesStatement().accept(adapter);
        new MySqlShowMasterStatusStatement().accept(adapter);
        new MySqlShowOpenTablesStatement().accept(adapter);
        new MySqlShowBinaryLogsStatement().accept(adapter);
        new MySqlShowPluginsStatement().accept(adapter);
        new MySqlShowPrivilegesStatement().accept(adapter);
        new MySqlShowProcedureCodeStatement().accept(adapter);
        new MySqlShowProcedureStatusStatement().accept(adapter);
        new MySqlShowProcessListStatement().accept(adapter);
        new MySqlShowProfileStatement().accept(adapter);
        new MySqlShowSlaveHostsStatement().accept(adapter);
        new MySqlShowRelayLogEventsStatement().accept(adapter);
        new MySqlShowSlaveStatusStatement().accept(adapter);
        new MySqlShowTableStatusStatement().accept(adapter);
        new MySqlShowTriggersStatement().accept(adapter);
        new MySqlRenameTableStatement().accept(adapter);
        new MySqlUseIndexHint().accept(adapter);
        new MySqlIgnoreIndexHint().accept(adapter);
        new MySqlLockTableStatement().accept(adapter);
        new MySqlUnlockTablesStatement().accept(adapter);
        new MySqlForceIndexHint().accept(adapter);
        new MySqlAlterTableChangeColumn().accept(adapter);
        new SQLAlterCharacter().accept(adapter);
        new MySqlAlterTableOption().accept(adapter);
        new MySqlCreateTableStatement().accept(adapter);
        new MySqlCharExpr().accept(adapter);
        new MySqlUnique().accept(adapter);
        new MySqlAlterTableModifyColumn().accept(adapter);
        new MySqlAlterTableDiscardTablespace().accept(adapter);
        new MySqlAlterTableImportTablespace().accept(adapter);
        new TableSpaceOption().accept(adapter);
    }
}
