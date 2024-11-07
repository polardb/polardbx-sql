package com.alibaba.polardbx.executor.ddl.newengine.subjob;

import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.factory.GsiTaskFactory;
import com.alibaba.polardbx.executor.handler.ddl.LogicalDropIndexHandler;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableRepartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RenameLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RepartitionPrepareData;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.ddl.AlterTableRepartition;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.when;

public class SpecialCharacterTest {

    @Test
    public void testGenAlterGlobalIndexDropColumnsSql() {
        String indexName = "idx`Name";
        List<String> columns = new ArrayList<>();
        columns.add("c`olumn");
        columns.add("c o l umn");
        columns.add("abc");
        String sql = GsiTaskFactory.genAlterGlobalIndexDropColumnsSql(indexName, columns);
        Assert.assertEquals(sql,
            "/*+TDDL:CMD_EXTRA(DDL_ON_GSI=true)*/alter table `idx``Name` drop column `c``olumn`, drop column `c o l umn`, drop column `abc`");
    }

    @Test
    public void testGenRenameLocalIndexTask() {
        RenameLocalIndexPreparedData preparedData = new RenameLocalIndexPreparedData();
        preparedData.setTableName("t`b");
        preparedData.setOrgIndexName("idx`Name");
        preparedData.setNewIndexName("new`Index");
        SubJobTask subJobTask = (SubJobTask) LogicalDropIndexHandler.genRenameLocalIndexTask(preparedData);
        Assert.assertEquals(subJobTask.getDdlStmt(),
            "/*+TDDL:cmd_extra(DDL_ON_GSI=true)*/alter table `t``b` rename index `idx``Name` to `new``Index`");
    }

    @Test
    public void testGenAddForeignKeySql() {
        SqlDdl sqlNode = Mockito.mock(SqlDdl.class);
        AlterTableRepartition alterTableRepartition = Mockito.mock(AlterTableRepartition.class);
        when(alterTableRepartition.getCluster()).thenReturn(Mockito.mock(RelOptCluster.class));
        when(alterTableRepartition.getTraitSet()).thenReturn(Mockito.mock(RelTraitSet.class));
        when(alterTableRepartition.getSqlNode()).thenReturn(sqlNode);
        when(sqlNode.toSqlString(Mockito.any())).thenReturn(new SqlString(Mockito.mock(SqlDialect.class), "123"));
        when(alterTableRepartition.getRowType()).thenReturn(Mockito.mock(RelDataType.class));

        Set<ForeignKeyData> foreignKeyDataList = new HashSet<>();
        ForeignKeyData foreignKeyData = new ForeignKeyData();
        foreignKeyData.setSchema("wumu");
        foreignKeyData.setTableName("t`b");
        foreignKeyData.setConstraint("c`onstraint");
        foreignKeyData.setIndexName("idx`Name");
        foreignKeyData.setColumns(new ArrayList<>());
        foreignKeyData.getColumns().add("c`olumn");
        foreignKeyData.setRefColumns(new ArrayList<>());
        foreignKeyData.getRefColumns().add("c`olumn");

        foreignKeyDataList.add(foreignKeyData);

        List<String> names = new ArrayList<>();
        names.add("wumu");
        names.add("t`b");
        try (MockedStatic<OptimizerContext> optimizerContextMockedStatic = Mockito.mockStatic(OptimizerContext.class)) {
            when(OptimizerContext.getContext(Mockito.anyString())).thenReturn(Mockito.mock(OptimizerContext.class));
            when(alterTableRepartition.getTableName()).thenReturn(new SqlIdentifier(names, SqlParserPos.ZERO));

            LogicalAlterTableRepartition logicalAlterTableRepartition =
                new LogicalAlterTableRepartition(alterTableRepartition);

            RepartitionPrepareData prepareData = new RepartitionPrepareData();

            Class<?> clazz = logicalAlterTableRepartition.getClass();
            Field secretField = clazz.getDeclaredField("repartitionPrepareData");
            secretField.setAccessible(true);
            secretField.set(logicalAlterTableRepartition, prepareData);

            logicalAlterTableRepartition.genAddForeignKeySql(foreignKeyDataList);
            Assert.assertEquals(prepareData.getAddForeignKeySql().get(0).getValue(),
                "ALTER TABLE `wumu`.`t``b` DROP FOREIGN KEY `c``onstraint` /* partition_fk_sub_job */");

            logicalAlterTableRepartition.genDropForeignKeySql(foreignKeyDataList);
            Assert.assertEquals(prepareData.getDropForeignKeySql().get(0).getKey(),
                "ALTER TABLE `wumu`.`t``b` DROP FOREIGN KEY `c``onstraint` /* partition_fk_sub_job */");

        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGenChangeLocalIndexSql4OptimizeKey() {
        SqlDdl sqlNode = Mockito.mock(SqlDdl.class);
        AlterTableRepartition alterTableRepartition = Mockito.mock(AlterTableRepartition.class);
        when(alterTableRepartition.getCluster()).thenReturn(Mockito.mock(RelOptCluster.class));
        when(alterTableRepartition.getTraitSet()).thenReturn(Mockito.mock(RelTraitSet.class));
        when(alterTableRepartition.getSqlNode()).thenReturn(sqlNode);
        when(sqlNode.toSqlString(Mockito.any())).thenReturn(new SqlString(Mockito.mock(SqlDialect.class), "123"));
        when(alterTableRepartition.getRowType()).thenReturn(Mockito.mock(RelDataType.class));

        List<String> names = new ArrayList<>();
        names.add("wumu");
        names.add("t`b");

        String tableDef = "CREATE TABLE `t1_swuq_00000` (\n"
            + "\t`a` int(11) DEFAULT NULL,\n"
            + "\t`b` int(11) DEFAULT NULL,\n"
            + "\t`_drds_implicit_id_` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "\tPRIMARY KEY (`_drds_implicit_id_`),\n"
            + "\tKEY `auto_shard_key_a` USING BTREE (`a`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4";

        String tableDef2 = "CREATE TABLE tbl (\n"
            + "\t`a` int(11) DEFAULT NULL,\n"
            + "\t`b` int(11) DEFAULT NULL,\n"
            + "\t`_drds_implicit_id_` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "\tPRIMARY KEY (`_drds_implicit_id_`),\n"
            + "\tINDEX `auto_shard_key_a_b` USING BTREE(`a`, `b`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4";

        try (MockedStatic<OptimizerContext> optimizerContextMockedStatic = Mockito.mockStatic(OptimizerContext.class)) {
            when(OptimizerContext.getContext(Mockito.anyString())).thenReturn(Mockito.mock(OptimizerContext.class));
            when(alterTableRepartition.getTableName()).thenReturn(new SqlIdentifier(names, SqlParserPos.ZERO));

            LogicalAlterTableRepartition logicalAlterTableRepartition =
                new LogicalAlterTableRepartition(alterTableRepartition);

            RepartitionPrepareData prepareData = new RepartitionPrepareData();

            Class<?> clazz = logicalAlterTableRepartition.getClass();
            Field secretField = clazz.getDeclaredField("repartitionPrepareData");
            secretField.setAccessible(true);
            secretField.set(logicalAlterTableRepartition, prepareData);

            logicalAlterTableRepartition.genDropIndexSql4OptimizeKey(tableDef);
            Assert.assertEquals(prepareData.getDropLocalIndexSql().getKey(),
                "alter table `wumu`.`t``b` drop index `auto_shard_key_a`;");

            List<String> newColumns = new ArrayList<>();
            newColumns.add("a");
            newColumns.add("b");

            logicalAlterTableRepartition.genChangeLocalIndexSql4OptimizeKey(tableDef2, newColumns);
            Assert.assertEquals(prepareData.getAddLocalIndexSql().getKey(),
                "alter table `wumu`.`t``b` add index `auto_shard_key_a_b` USING BTREE (a,b);");
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}
