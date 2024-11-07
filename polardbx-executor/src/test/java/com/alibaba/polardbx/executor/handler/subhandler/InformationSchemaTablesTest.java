package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.atom.config.TAtomDsConfDO;
import com.alibaba.polardbx.atom.config.TAtomDsConfHandle;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.spi.ITopologyExecutor;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.group.config.OptimizedGroupConfigManager;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.rpc.compatible.ArrayResultSet;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.TddlRule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.gms.topology.DbTopologyManager.getConnectionForStorage;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class InformationSchemaTablesTest {
    @Before
    public void before() {
        InstanceVersion.setMYSQL80(true);
    }

    @Before
    public void after() {
        InstanceVersion.setMYSQL80(false);
    }

    @Test
    public void test() throws SQLException {
        try (MockedStatic<DbTopologyManager> mockDbTopologyManager = mockStatic(DbTopologyManager.class)) {
            // mock mock
            // db1: drds mode, contains table tb1, tb1 contains gsi gsi1
            // db2: auto mode, contains table tb2, tb2 contains gsi gsi2
            String db1 = "db1";
            String db1Group1 = "db1_00000_GROUP";
            String db1Group2 = "db1_00001_GROUP";
            String db2 = "db2";
            String db2Group1 = "db2_00000_GROUP";
            String db2Group2 = "db2_00001_GROUP";
            String db2db1 = "db2_00000";
            String db2db2 = "db2_00001";
            String tb1 = "tb1";
            String tb1p1 = "tb1_qwer";
            String tb1p2 = "tb1_rtyu";
            String tb2 = "tb2";
            String tb2p1 = "tb2_uiop";
            String tb2p2 = "tb2_asdf";
            String gsi1 = "gsi1";
            String gsi1p1 = "gsi1_hjkl";
            String gsi1p2 = "gsi1_qwer";
            String gsi2 = "gsi2";
            String gsi2p1 = "gsi2_hjkl";
            String gsi2p2 = "gsi2_qwer";

            Set<String> schemaNames = ImmutableSet.of(db1, db2);
            Set<String> tables = ImmutableSet.of(tb1, tb2);

            // mock db1
            ExecutorContext executorContext1 = new ExecutorContext();
            ITopologyExecutor topologyExecutor1 = mock(ITopologyExecutor.class);
            executorContext1.setTopologyExecutor(topologyExecutor1);
            ExecutorContext.setContext(db1, executorContext1);

            IGroupExecutor groupExecutor1 = mock(IGroupExecutor.class);
            when(topologyExecutor1.getGroupExecutor(db1Group1)).thenReturn(groupExecutor1);
            TGroupDataSource dataSource1 = mock(TGroupDataSource.class);
            when(groupExecutor1.getDataSource()).thenReturn(dataSource1);
            String dataSource1Address = "127.0.0.1:3001";
            when(dataSource1.getMasterSourceAddress()).thenReturn(dataSource1Address);
            when(dataSource1.getMasterDNId()).thenReturn(dataSource1Address);
            OptimizedGroupConfigManager configManager1 = mock(OptimizedGroupConfigManager.class);
            when(dataSource1.getConfigManager()).thenReturn(configManager1);
            TAtomDataSource tAtomDataSource1 = mock(TAtomDataSource.class);
            when(configManager1.getDataSource(any())).thenReturn(tAtomDataSource1);
            TAtomDsConfHandle tAtomDsConfHandle1 = mock(TAtomDsConfHandle.class);
            when(tAtomDataSource1.getDsConfHandle()).thenReturn(tAtomDsConfHandle1);
            TAtomDsConfDO tAtomDsConfDO1 = mock(TAtomDsConfDO.class);
            when(tAtomDsConfHandle1.getRunTimeConf()).thenReturn(tAtomDsConfDO1);
            when(tAtomDsConfDO1.getDbName()).thenReturn(db1Group1);
            Connection connection1 = mock(Connection.class);
            mockDbTopologyManager.when(() -> getConnectionForStorage(dataSource1Address)).thenReturn(connection1);
            Statement statement1 = mock(Statement.class);
            when(connection1.createStatement()).thenReturn(statement1);
            ResultSet rs1 = mock2Results(db1Group1.toLowerCase(), tb1p1.toLowerCase(), db1Group1.toLowerCase(),
                gsi1p1.toLowerCase());
            when(statement1.executeQuery(any())).then(invocation -> {
                String sql = invocation.getArgument(0);
                System.out.println(sql);
                Assert.assertTrue(sql.contains("('" + db1Group1.toLowerCase() + "', '" + tb1p1.toLowerCase() + "')"));
                Assert.assertTrue(sql.contains("('" + db1Group1.toLowerCase() + "', '" + gsi1p1.toLowerCase() + "'))"));
                return rs1;
            });

            IGroupExecutor groupExecutor2 = mock(IGroupExecutor.class);
            when(topologyExecutor1.getGroupExecutor(db1Group2)).thenReturn(groupExecutor2);
            TGroupDataSource dataSource2 = mock(TGroupDataSource.class);
            when(groupExecutor2.getDataSource()).thenReturn(dataSource2);
            String dataSource2Address = "127.0.0.1:3002";
            when(dataSource2.getMasterSourceAddress()).thenReturn(dataSource2Address);
            when(dataSource2.getMasterDNId()).thenReturn(dataSource2Address);
            OptimizedGroupConfigManager configManager2 = mock(OptimizedGroupConfigManager.class);
            when(dataSource2.getConfigManager()).thenReturn(configManager2);
            TAtomDataSource tAtomDataSource2 = mock(TAtomDataSource.class);
            when(configManager2.getDataSource(any())).thenReturn(tAtomDataSource2);
            TAtomDsConfHandle tAtomDsConfHandle2 = mock(TAtomDsConfHandle.class);
            when(tAtomDataSource2.getDsConfHandle()).thenReturn(tAtomDsConfHandle2);
            TAtomDsConfDO tAtomDsConfDO2 = mock(TAtomDsConfDO.class);
            when(tAtomDsConfHandle2.getRunTimeConf()).thenReturn(tAtomDsConfDO2);
            when(tAtomDsConfDO2.getDbName()).thenReturn(db1Group2);
            Connection connection2 = mock(Connection.class);
            mockDbTopologyManager.when(() -> getConnectionForStorage(dataSource2Address)).thenReturn(connection2);
            Statement statement2 = mock(Statement.class);
            when(connection2.createStatement()).thenReturn(statement2);
            ResultSet rs2 = mock2Results(db1Group2.toLowerCase(), tb1p2.toLowerCase(), db1Group2.toLowerCase(),
                gsi1p2.toLowerCase());
            when(statement2.executeQuery(any())).then(invocation -> {
                String sql = invocation.getArgument(0);
                System.out.println(sql);
                Assert.assertTrue(sql.contains("('" + db1Group2.toLowerCase() + "', '" + tb1p2.toLowerCase() + "')"));
                Assert.assertTrue(sql.contains("('" + db1Group2.toLowerCase() + "', '" + gsi1p2.toLowerCase() + "'))"));
                return rs2;
            });

            OptimizerContext optimizerContext1 = new OptimizerContext(db1);
            OptimizerContext.loadContext(optimizerContext1);
            // mock tddl rule manager
            TddlRuleManager ruleManager1 = mock(TddlRuleManager.class);
            optimizerContext1.setRuleManager(ruleManager1);
            TddlRule tddlRule = mock(TddlRule.class);
            when(ruleManager1.getTddlRule()).thenReturn(tddlRule);
            TableRule tb1TableRule = mock(TableRule.class);
            TableRule gsi1TableRule = mock(TableRule.class);
            Collection<TableRule> tableRules = ImmutableSet.of(tb1TableRule, gsi1TableRule);
            when(tddlRule.getTables()).thenReturn(tableRules);
            when(tb1TableRule.getVirtualTbName()).thenReturn(tb1);
            when(gsi1TableRule.getVirtualTbName()).thenReturn(gsi1);
            when(ruleManager1.getTableRule(tb1)).thenReturn(tb1TableRule);
            when(ruleManager1.getTableRule(gsi1)).thenReturn(gsi1TableRule);
            Map<String, Set<String>> tb1Topologys = ImmutableMap.of(
                db1Group1, ImmutableSet.of(tb1p1),
                db1Group2, ImmutableSet.of(tb1p2)
            );
            when(tb1TableRule.getStaticTopology()).thenReturn(tb1Topologys);
            Map<String, Set<String>> gsi1Topologys = ImmutableMap.of(
                db1Group1, ImmutableSet.of(gsi1p1),
                db1Group2, ImmutableSet.of(gsi1p2)
            );
            when(gsi1TableRule.getStaticTopology()).thenReturn(gsi1Topologys);
            // mock empty partition manager
            PartitionInfoManager partitionInfoManager1 = mock(PartitionInfoManager.class);
            optimizerContext1.setPartitionInfoManager(partitionInfoManager1);
            when(partitionInfoManager1.getPartitionInfos()).thenReturn(new ArrayList<>());
            // mock schema manager and table meta
            SchemaManager schemaManager1 = mock(SchemaManager.class);
            optimizerContext1.setSchemaManager(schemaManager1);
            TableMeta tb1TableMeta = mock(TableMeta.class);
            when(schemaManager1.getTable(tb1)).thenReturn(tb1TableMeta);
            GsiMetaManager.GsiIndexMetaBean gsi1IndexMeta = mock(GsiMetaManager.GsiIndexMetaBean.class);
            Map<String, GsiMetaManager.GsiIndexMetaBean> tb1Gsi = ImmutableMap.of(
                gsi1, gsi1IndexMeta
            );
            when(tb1TableMeta.getGsiPublished()).thenReturn(tb1Gsi);

            // mock db2
            ExecutorContext executorContext2 = new ExecutorContext();
            ITopologyExecutor topologyExecutor2 = mock(ITopologyExecutor.class);
            ExecutorContext.setContext(db2, executorContext2);
            executorContext2.setTopologyExecutor(topologyExecutor2);

            IGroupExecutor groupExecutor3 = mock(IGroupExecutor.class);
            when(topologyExecutor2.getGroupExecutor(db2Group1)).thenReturn(groupExecutor3);
            TGroupDataSource dataSource3 = mock(TGroupDataSource.class);
            when(groupExecutor3.getDataSource()).thenReturn(dataSource3);
            String dataSource3Address = "127.0.0.1:3003";
            when(dataSource3.getMasterSourceAddress()).thenReturn(dataSource3Address);
            when(dataSource3.getMasterDNId()).thenReturn(dataSource3Address);
            Connection connection3 = mock(Connection.class);
            mockDbTopologyManager.when(() -> getConnectionForStorage(dataSource3Address)).thenReturn(connection3);
            Statement statement3 = mock(Statement.class);
            when(connection3.createStatement()).thenReturn(statement3);
            ResultSet rs3 = mock2Results(db2db1.toLowerCase(), tb2p1.toLowerCase(), db2db1.toLowerCase(),
                gsi2p1.toLowerCase());
            when(statement3.executeQuery(any())).then(invocation -> {
                String sql = invocation.getArgument(0);
                System.out.println(sql);
                Assert.assertTrue(sql.contains("('" + db2db1.toLowerCase() + "', '" + tb2p1.toLowerCase() + "')"));
                Assert.assertTrue(sql.contains("('" + db2db1.toLowerCase() + "', '" + gsi2p1.toLowerCase() + "'))"));
                return rs3;
            });

            IGroupExecutor groupExecutor4 = mock(IGroupExecutor.class);
            when(topologyExecutor2.getGroupExecutor(db2Group2)).thenReturn(groupExecutor4);
            TGroupDataSource dataSource4 = mock(TGroupDataSource.class);
            when(groupExecutor4.getDataSource()).thenReturn(dataSource4);
            String dataSource4Address = "127.0.0.1:3004";
            when(dataSource4.getMasterSourceAddress()).thenReturn(dataSource4Address);
            when(dataSource4.getMasterDNId()).thenReturn(dataSource4Address);
            Connection connection4 = mock(Connection.class);
            mockDbTopologyManager.when(() -> getConnectionForStorage(dataSource4Address)).thenReturn(connection4);
            Statement statement4 = mock(Statement.class);
            when(connection4.createStatement()).thenReturn(statement4);
            ResultSet rs4 = mock2Results(db2db2.toLowerCase(), tb2p2.toLowerCase(), db2db2.toLowerCase(),
                gsi2p2.toLowerCase());
            when(statement4.executeQuery(any())).then(invocation -> {
                String sql = invocation.getArgument(0);
                System.out.println(sql);
                Assert.assertTrue(sql.contains("('" + db2db2.toLowerCase() + "', '" + tb2p2.toLowerCase() + "')"));
                Assert.assertTrue(sql.contains("('" + db2db2.toLowerCase() + "', '" + gsi2p2.toLowerCase() + "'))"));
                return rs4;
            });

            OptimizerContext optimizerContext2 = new OptimizerContext(db2);
            OptimizerContext.loadContext(optimizerContext2);
            // mock empty tddl rule manager
            TddlRuleManager ruleManager2 = mock(TddlRuleManager.class);
            optimizerContext2.setRuleManager(ruleManager2);
            TddlRule tddlRule2 = mock(TddlRule.class);
            when(ruleManager2.getTddlRule()).thenReturn(tddlRule2);
            when(tddlRule2.getTables()).thenReturn(new ArrayList<>());
            // mock partition manager
            PartitionInfoManager partitionInfoManager2 = mock(PartitionInfoManager.class);
            optimizerContext2.setPartitionInfoManager(partitionInfoManager2);
            PartitionInfo partitionInfo1 = mock(PartitionInfo.class);
            PartitionInfo partitionInfo2 = mock(PartitionInfo.class);
            when(partitionInfoManager2.getPartitionInfos()).thenReturn(
                ImmutableList.of(partitionInfo1, partitionInfo2));
            when(partitionInfo1.getTableName()).thenReturn(tb2);
            when(partitionInfo2.getTableName()).thenReturn(gsi2);
            when(partitionInfoManager2.getPartitionInfo(tb2)).thenReturn(partitionInfo1);
            when(partitionInfoManager2.getPartitionInfo(gsi2)).thenReturn(partitionInfo2);
            PartitionByDefinition partitionByDefinition1 = mock(PartitionByDefinition.class);
            when(partitionInfo1.getPartitionBy()).thenReturn(partitionByDefinition1);
            PartitionSpec tb2PartitionSpec1 = mock(PartitionSpec.class);
            PartitionSpec tb2PartitionSpec2 = mock(PartitionSpec.class);
            when(partitionByDefinition1.getPhysicalPartitions()).thenReturn(
                ImmutableList.of(tb2PartitionSpec1, tb2PartitionSpec2));
            PartitionLocation tb2Location1 = mock(PartitionLocation.class);
            PartitionLocation tb2Location2 = mock(PartitionLocation.class);
            when(tb2Location1.getGroupKey()).thenReturn(db2Group1);
            when(tb2Location2.getGroupKey()).thenReturn(db2Group2);
            when(tb2Location1.getPhyTableName()).thenReturn(tb2p1);
            when(tb2Location2.getPhyTableName()).thenReturn(tb2p2);
            when(tb2PartitionSpec1.getLocation()).thenReturn(tb2Location1);
            when(tb2PartitionSpec2.getLocation()).thenReturn(tb2Location2);

            PartitionByDefinition partitionByDefinition2 = mock(PartitionByDefinition.class);
            when(partitionInfo2.getPartitionBy()).thenReturn(partitionByDefinition2);
            PartitionSpec gsi2PartitionSpec1 = mock(PartitionSpec.class);
            PartitionSpec gsi2PartitionSpec2 = mock(PartitionSpec.class);
            when(partitionByDefinition2.getPhysicalPartitions()).thenReturn(
                ImmutableList.of(gsi2PartitionSpec1, gsi2PartitionSpec2));
            PartitionLocation gsi2Location1 = mock(PartitionLocation.class);
            PartitionLocation gsi2Location2 = mock(PartitionLocation.class);
            when(gsi2Location1.getGroupKey()).thenReturn(db2Group1);
            when(gsi2Location2.getGroupKey()).thenReturn(db2Group2);
            when(gsi2Location1.getPhyTableName()).thenReturn(gsi2p1);
            when(gsi2Location2.getPhyTableName()).thenReturn(gsi2p2);
            when(gsi2PartitionSpec1.getLocation()).thenReturn(gsi2Location1);
            when(gsi2PartitionSpec2.getLocation()).thenReturn(gsi2Location2);

            // mock schema manager and table meta
            SchemaManager schemaManager2 = mock(SchemaManager.class);
            optimizerContext2.setSchemaManager(schemaManager2);
            TableMeta tb2TableMeta = mock(TableMeta.class);
            when(schemaManager2.getTable(tb2)).thenReturn(tb2TableMeta);
            GsiMetaManager.GsiIndexMetaBean gsi2IndexMeta = mock(GsiMetaManager.GsiIndexMetaBean.class);
            Map<String, GsiMetaManager.GsiIndexMetaBean> tb2Gsi = ImmutableMap.of(
                gsi2, gsi2IndexMeta
            );
            when(tb2TableMeta.getGsiPublished()).thenReturn(tb2Gsi);
            when(partitionInfoManager2.isNewPartDbTable(tb2)).thenReturn(true);

            String tableLike = null;
            ArrayResultCursor cursor = new ArrayResultCursor("TABLES");
            InformationSchemaTablesHandler.fetchAccurateInfoSchemaTables(schemaNames, tables, tableLike, cursor);
            for (Row row : cursor.getRows()) {
                String table = (String) row.getObject(2);
                String schema = (String) row.getObject(1);
                long dataLength = (long) row.getObject(9);
                long indexLength = (long) row.getObject(11);
                if (table.equals(tb1)) {
                    Assert.assertEquals(db1, schema);
                    Assert.assertEquals(200, dataLength);
                    Assert.assertEquals(400, indexLength);
                } else if (table.equals(gsi1)) {
                    Assert.assertEquals(db1, schema);
                    Assert.assertEquals(200, dataLength);
                    Assert.assertEquals(0, indexLength);
                } else if (table.equals(tb2)) {
                    Assert.assertEquals(db2, schema);
                    Assert.assertEquals(200, dataLength);
                    Assert.assertEquals(400, indexLength);
                } else if (table.equals(gsi2)) {
                    Assert.assertEquals(db2, schema);
                    Assert.assertEquals(200, dataLength);
                    Assert.assertEquals(0, indexLength);
                }
            }
        }
    }

    @Test
    public void testLike() throws SQLException {
        try (MockedStatic<DbTopologyManager> mockDbTopologyManager = mockStatic(DbTopologyManager.class)) {
            // mock mock
            // db1: drds mode, contains table tb1, tb1 contains gsi gsi1
            // db2: auto mode, contains table tb2, tb2 contains gsi gsi2
            String db1 = "db1";
            String db1Group1 = "db1_00000_GROUP";
            String db1Group2 = "db1_00001_GROUP";
            String tb1 = "tb1";
            String tb1p1 = "tb1_qwer";
            String tb1p2 = "tb1_rtyu";

            Set<String> schemaNames = ImmutableSet.of(db1);
            Set<String> tables = ImmutableSet.of(tb1);

            // mock db1
            ExecutorContext executorContext1 = new ExecutorContext();
            ITopologyExecutor topologyExecutor1 = mock(ITopologyExecutor.class);
            executorContext1.setTopologyExecutor(topologyExecutor1);
            ExecutorContext.setContext(db1, executorContext1);

            IGroupExecutor groupExecutor1 = mock(IGroupExecutor.class);
            when(topologyExecutor1.getGroupExecutor(db1Group1)).thenReturn(groupExecutor1);
            TGroupDataSource dataSource1 = mock(TGroupDataSource.class);
            when(groupExecutor1.getDataSource()).thenReturn(dataSource1);
            String dataSource1Address = "127.0.0.1:3001";
            when(dataSource1.getMasterSourceAddress()).thenReturn(dataSource1Address);
            when(dataSource1.getMasterDNId()).thenReturn(dataSource1Address);
            OptimizedGroupConfigManager configManager1 = mock(OptimizedGroupConfigManager.class);
            when(dataSource1.getConfigManager()).thenReturn(configManager1);
            TAtomDataSource tAtomDataSource1 = mock(TAtomDataSource.class);
            when(configManager1.getDataSource(any())).thenReturn(tAtomDataSource1);
            TAtomDsConfHandle tAtomDsConfHandle1 = mock(TAtomDsConfHandle.class);
            when(tAtomDataSource1.getDsConfHandle()).thenReturn(tAtomDsConfHandle1);
            TAtomDsConfDO tAtomDsConfDO1 = mock(TAtomDsConfDO.class);
            when(tAtomDsConfHandle1.getRunTimeConf()).thenReturn(tAtomDsConfDO1);
            when(tAtomDsConfDO1.getDbName()).thenReturn(db1Group1);
            Connection connection1 = mock(Connection.class);
            mockDbTopologyManager.when(() -> getConnectionForStorage(dataSource1Address)).thenReturn(connection1);
            Statement statement1 = mock(Statement.class);
            when(connection1.createStatement()).thenReturn(statement1);
            ResultSet rs1 = mock1Result1(db1Group1.toLowerCase(), tb1p1.toLowerCase());
            when(statement1.executeQuery(any())).then(invocation -> {
                String sql = invocation.getArgument(0);
                System.out.println(sql);
                Assert.assertTrue(sql.contains("('" + db1Group1.toLowerCase() + "', '" + tb1p1.toLowerCase() + "')"));
                return rs1;
            });

            IGroupExecutor groupExecutor2 = mock(IGroupExecutor.class);
            when(topologyExecutor1.getGroupExecutor(db1Group2)).thenReturn(groupExecutor2);
            TGroupDataSource dataSource2 = mock(TGroupDataSource.class);
            when(groupExecutor2.getDataSource()).thenReturn(dataSource2);
            String dataSource2Address = "127.0.0.1:3002";
            when(dataSource2.getMasterSourceAddress()).thenReturn(dataSource2Address);
            when(dataSource2.getMasterDNId()).thenReturn(dataSource2Address);
            OptimizedGroupConfigManager configManager2 = mock(OptimizedGroupConfigManager.class);
            when(dataSource2.getConfigManager()).thenReturn(configManager2);
            TAtomDataSource tAtomDataSource2 = mock(TAtomDataSource.class);
            when(configManager2.getDataSource(any())).thenReturn(tAtomDataSource2);
            TAtomDsConfHandle tAtomDsConfHandle2 = mock(TAtomDsConfHandle.class);
            when(tAtomDataSource2.getDsConfHandle()).thenReturn(tAtomDsConfHandle2);
            TAtomDsConfDO tAtomDsConfDO2 = mock(TAtomDsConfDO.class);
            when(tAtomDsConfHandle2.getRunTimeConf()).thenReturn(tAtomDsConfDO2);
            when(tAtomDsConfDO2.getDbName()).thenReturn(db1Group2);
            Connection connection2 = mock(Connection.class);
            mockDbTopologyManager.when(() -> getConnectionForStorage(dataSource2Address)).thenReturn(connection2);
            Statement statement2 = mock(Statement.class);
            when(connection2.createStatement()).thenReturn(statement2);
            ResultSet rs2 = mock1Result1(db1Group2.toLowerCase(), tb1p2.toLowerCase());
            when(statement2.executeQuery(any())).then(invocation -> {
                String sql = invocation.getArgument(0);
                System.out.println(sql);
                Assert.assertTrue(sql.contains("('" + db1Group2.toLowerCase() + "', '" + tb1p2.toLowerCase() + "')"));
                return rs2;
            });

            OptimizerContext optimizerContext1 = new OptimizerContext(db1);
            OptimizerContext.loadContext(optimizerContext1);
            // mock tddl rule manager
            TddlRuleManager ruleManager1 = mock(TddlRuleManager.class);
            optimizerContext1.setRuleManager(ruleManager1);
            TddlRule tddlRule = mock(TddlRule.class);
            when(ruleManager1.getTddlRule()).thenReturn(tddlRule);
            TableRule tb1TableRule = mock(TableRule.class);
            Collection<TableRule> tableRules = ImmutableSet.of(tb1TableRule);
            when(tddlRule.getTables()).thenReturn(tableRules);
            when(tb1TableRule.getVirtualTbName()).thenReturn(tb1);
            when(ruleManager1.getTableRule(tb1)).thenReturn(tb1TableRule);
            Map<String, Set<String>> tb1Topologys = ImmutableMap.of(
                db1Group1, ImmutableSet.of(tb1p1),
                db1Group2, ImmutableSet.of(tb1p2)
            );
            when(tb1TableRule.getStaticTopology()).thenReturn(tb1Topologys);
            // mock empty partition manager
            PartitionInfoManager partitionInfoManager1 = mock(PartitionInfoManager.class);
            optimizerContext1.setPartitionInfoManager(partitionInfoManager1);
            when(partitionInfoManager1.getPartitionInfos()).thenReturn(new ArrayList<>());
            // mock schema manager and table meta
            SchemaManager schemaManager1 = mock(SchemaManager.class);
            optimizerContext1.setSchemaManager(schemaManager1);
            TableMeta tb1TableMeta = mock(TableMeta.class);
            when(schemaManager1.getTable(tb1)).thenReturn(tb1TableMeta);
            when(tb1TableMeta.getGsiPublished()).thenReturn(null);

            String tableLike = "%t%";
            ArrayResultCursor cursor = new ArrayResultCursor("TABLES");
            InformationSchemaTablesHandler.fetchAccurateInfoSchemaTables(schemaNames, tables, tableLike, cursor);
            for (Row row : cursor.getRows()) {
                String table = (String) row.getObject(2);
                String schema = (String) row.getObject(1);
                long dataLength = (long) row.getObject(9);
                long indexLength = (long) row.getObject(11);
                if (table.equals(tb1)) {
                    Assert.assertEquals(db1, schema);
                    Assert.assertEquals(200, dataLength);
                    Assert.assertEquals(200, indexLength);
                }
            }
        }

    }

    private ArrayResultSet mock1Result1(String db1, String tb1) {
        ArrayResultSet rs = mockColumn();
        rs.getRows().add(mockRow(db1, tb1));
        return rs;
    }

    private ArrayResultSet mock2Results(String db1, String tb1, String db2, String tb2) {
        ArrayResultSet rs = mockColumn();
        rs.getRows().add(mockRow(db1, tb1));
        rs.getRows().add(mockRow(db2, tb2));
        return rs;
    }

    private ArrayResultSet mockColumn() {
        ArrayResultSet rs = new ArrayResultSet();
        rs.getColumnName().add("TABLE_SCHEMA");
        rs.getColumnName().add("TABLE_NAME");
        rs.getColumnName().add("VERSION");
        rs.getColumnName().add("ROW_FORMAT");
        rs.getColumnName().add("TABLE_ROWS");
        rs.getColumnName().add("AVG_ROW_LENGTH");
        rs.getColumnName().add("DATA_LENGTH");
        rs.getColumnName().add("MAX_DATA_LENGTH");
        rs.getColumnName().add("INDEX_LENGTH");
        rs.getColumnName().add("DATA_FREE");
        rs.getColumnName().add("AUTO_INCREMENT");
        rs.getColumnName().add("CREATE_TIME");
        rs.getColumnName().add("UPDATE_TIME");
        rs.getColumnName().add("CHECK_TIME");
        rs.getColumnName().add("TABLE_COLLATION");
        rs.getColumnName().add("CHECKSUM");
        rs.getColumnName().add("TABLE_COMMENT");
        rs.getColumnName().add("CREATE_OPTIONS");
        return rs;
    }

    private Object[] mockRow(String db, String tb) {
        return new Object[] {
            db,
            tb,
            10L,
            "Dynamic",
            100L,
            64L,
            100L,
            0L,
            100L,
            100L,
            "0",
            "2024-01-01 00:00:00",
            "2024-01-01 00:00:00",
            "2024-01-01 00:00:00",
            "utf8_general_ci",
            "",
            "",
            ""
        };
    }

}
