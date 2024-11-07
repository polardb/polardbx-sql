package com.alibaba.polardbx.executor.statistics;

import com.alibaba.polardbx.common.properties.BooleanConfigParam;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.mpp.deploy.Server;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.statistic.ndv.NDVShardSketch;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.node.AllNodes;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.MppScope;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.ImmutableBiMap;
import com.sun.tools.javac.util.List;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class SketchByColumnarTest {

    @Test
    public void testGenColumnarHllHintWithoutEc() {

        try (MockedStatic<InstConfUtil> instConfUtilMockedStatic = Mockito.mockStatic(InstConfUtil.class)) {
            instConfUtilMockedStatic.when(() -> InstConfUtil.getBool(any(BooleanConfigParam.class))).thenReturn(false);
            assertThat(NDVShardSketch.genColumnarHllHint(null)).isNull();
        }

        try (MockedStatic<InstConfUtil> instConfUtilMockedStatic = Mockito.mockStatic(InstConfUtil.class)) {
            instConfUtilMockedStatic.when(() -> InstConfUtil.getBool(any(BooleanConfigParam.class))).thenAnswer(
                invocation -> {
                    BooleanConfigParam param = invocation.getArgument(0);
                    if (param.equals(ConnectionParams.ENABLE_MPP_NDV_USE_COLUMNAR)) {
                        return true;
                    }
                    return false;
                }
            );

            String hint = NDVShardSketch.genColumnarHllHint(null);
            assertThat(hint).isNotEmpty();
            assertThat(hint).contains("ENABLE_DIRECT_PLAN=false");
            assertThat(hint).contains("ENABLE_HTAP=true");
        }
    }

    @Test
    public void testGenColumnarHllHint() {
        ExecutionContext ec = mock(ExecutionContext.class);
        ParamManager pm = mock(ParamManager.class);
        Mockito.when(ec.getParamManager()).thenReturn(pm);

        when(pm.getBoolean(any(BooleanConfigParam.class))).thenReturn(false);
        assertThat(NDVShardSketch.genColumnarHllHint(ec)).isNull();

        String hint;

        when(pm.getBoolean(any(BooleanConfigParam.class))).thenAnswer(
            invocation -> {
                BooleanConfigParam param = invocation.getArgument(0);
                if (param.equals(ConnectionParams.ENABLE_MPP_NDV_USE_COLUMNAR)) {
                    return true;
                }
                return false;
            });
        hint = NDVShardSketch.genColumnarHllHint(ec);
        assertThat(hint).contains("ENABLE_MASTER_MPP=true");

        when(pm.getBoolean(any(BooleanConfigParam.class))).thenAnswer(
            invocation -> {
                BooleanConfigParam param = invocation.getArgument(0);
                if (param.equals(ConnectionParams.ENABLE_MPP_NDV_USE_COLUMNAR)) {
                    return true;
                }
                return false;
            });
        hint = NDVShardSketch.genColumnarHllHint(ec);
        assertThat(hint).contains("ENABLE_MASTER_MPP=true");
    }

    @Test
    public void testGenColumnarHllLimitHint() {
        ExecutionContext ec = mock(ExecutionContext.class);
        ParamManager pm = mock(ParamManager.class);
        Mockito.when(ec.getParamManager()).thenReturn(pm);

        when(pm.getBoolean(any(BooleanConfigParam.class))).thenReturn(false);
        assertThat(NDVShardSketch.genColumnarHllHint(ec)).isNull();

        when(pm.getBoolean(any(BooleanConfigParam.class))).thenAnswer(
            invocation -> {
                BooleanConfigParam param = invocation.getArgument(0);
                if (param.equals(ConnectionParams.ENABLE_MPP_NDV_USE_COLUMNAR)) {
                    return true;
                }
                if (param.equals(ConnectionParams.MPP_NDV_USE_COLUMNAR_LIMIT)) {
                    return true;
                }
                return false;
            });

        try (MockedStatic<ExecUtils> execUtilsMockedStatic = Mockito.mockStatic(ExecUtils.class);
            final MockedStatic<ServiceProvider> mockServiceProvider = mockStatic(ServiceProvider.class)) {
            execUtilsMockedStatic.when(() -> ExecUtils.getMppSchedulerScope(any(Boolean.class))).thenAnswer(
                invocation -> MppScope.CURRENT
            );
            ServiceProvider serviceProvider = mock(ServiceProvider.class);
            mockServiceProvider.when(ServiceProvider::getInstance).thenReturn(serviceProvider);
            Server server = mock(Server.class);
            when(serviceProvider.getServer()).thenReturn(server);
            InternalNodeManager nodeManager = mock(InternalNodeManager.class);
            when(server.getNodeManager()).thenReturn(nodeManager);
            AllNodes allNodes = mock(AllNodes.class);
            when(nodeManager.getAllNodes()).thenReturn(allNodes);
            when(allNodes.getAllWorkers(any(MppScope.class))).thenAnswer(
                invocation -> List.of(mock(InternalNode.class), mock(InternalNode.class))
            );

            String hint;
            hint = NDVShardSketch.genColumnarHllHint(ec);
            assertThat(hint).contains("MPP_PARALLELISM=2");
            assertThat(hint).contains("MPP_NODE_SIZE=2");

            execUtilsMockedStatic.when(() -> ExecUtils.getMppSchedulerScope(any(Boolean.class))).thenAnswer(
                invocation -> MppScope.COLUMNAR
            );
            when(pm.getBoolean(any(BooleanConfigParam.class))).thenAnswer(
                invocation -> {
                    BooleanConfigParam param = invocation.getArgument(0);
                    if (param.equals(ConnectionParams.ENABLE_MPP_NDV_USE_COLUMNAR)) {
                        return true;
                    }
                    if (param.equals(ConnectionParams.MPP_NDV_USE_COLUMNAR_LIMIT)) {
                        return true;
                    }
                    return false;
                });
            hint = NDVShardSketch.genColumnarHllHint(ec);
            assertThat(hint).doesNotContain("MPP_PARALLELISM");

            when(pm.getBoolean(any(BooleanConfigParam.class))).thenAnswer(
                invocation -> {
                    BooleanConfigParam param = invocation.getArgument(0);
                    if (param.equals(ConnectionParams.ENABLE_MPP_NDV_USE_COLUMNAR)) {
                        return true;
                    }
                    return false;
                });
            hint = NDVShardSketch.genColumnarHllHint(ec);
            assertThat(hint).doesNotContain("MPP_PARALLELISM");
        }
    }

    @Test
    public void testGenColumnarHllMeta() {
        try (MockedStatic<OptimizerContext> optimizerContextMockedStatic = Mockito.mockStatic(OptimizerContext.class);
            MockedStatic<InstConfUtil> instConfUtilMockedStatic = Mockito.mockStatic(InstConfUtil.class)) {
            String hint;
            TableMeta tableMeta = mock(TableMeta.class);
            SchemaManager schemaManager = mock(SchemaManager.class);
            OptimizerContext optimizerContext = mock(OptimizerContext.class);
            optimizerContextMockedStatic.when(() -> OptimizerContext.getContext(any())).thenReturn(optimizerContext);

            when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
            when(schemaManager.getTableWithNull(any())).thenReturn(null);
            hint = NDVShardSketch.genColumnarHllHint(null, "a", "b");
            assertThat(hint).isNull();

            when(schemaManager.getTableWithNull(any())).thenReturn(tableMeta);
            when(tableMeta.getColumnarIndexPublished()).thenReturn(null);
            hint = NDVShardSketch.genColumnarHllHint(null, "a", "b");
            assertThat(hint).isNull();

            when(tableMeta.getColumnarIndexPublished()).thenReturn(ImmutableBiMap.of("c",
                mock(GsiMetaManager.GsiIndexMetaBean.class)));
            instConfUtilMockedStatic.when(() -> InstConfUtil.getBool(any())).thenReturn(false);
            hint = NDVShardSketch.genColumnarHllHint(null, "a", "b");
            assertThat(hint).isNull();
        }
    }
}
