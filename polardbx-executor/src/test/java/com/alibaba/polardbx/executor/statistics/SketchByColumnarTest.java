package com.alibaba.polardbx.executor.statistics;

import com.alibaba.polardbx.common.properties.BooleanConfigParam;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.statistic.ndv.NDVShardSketch;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
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
                    if (param.equals(ConnectionParams.ENABLE_NDV_USE_COLUMNAR)) {
                        return true;
                    }
                    return false;
                }
            );

            String hint = NDVShardSketch.genColumnarHllHint(null);
            assertThat(hint).isNotEmpty();
            assertThat(hint).contains("ENABLE_DIRECT_PLAN=false");
            assertThat(hint).doesNotContain("ENABLE_HTAP");
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

        // enable ENABLE_NDV_USE_COLUMNAR
        when(pm.getBoolean(any(BooleanConfigParam.class))).thenAnswer(
            invocation -> {
                BooleanConfigParam param = invocation.getArgument(0);
                if (param.equals(ConnectionParams.ENABLE_NDV_USE_COLUMNAR)) {
                    return true;
                }
                return false;
            });

        String hint;
        hint = NDVShardSketch.genColumnarHllHint(ec);
        assertThat(NDVShardSketch.genColumnarHllHint(ec)).isNotEmpty();
        assertThat(hint).doesNotContain("ENABLE_HTAP");
        assertThat(hint).doesNotContain("ENABLE_MASTER_MPP");

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
                if (param.equals(ConnectionParams.ENABLE_NDV_USE_COLUMNAR)) {
                    return true;
                }
                if (param.equals(ConnectionParams.ENABLE_MPP_NDV_USE_COLUMNAR)) {
                    return true;
                }
                return false;
            });
        hint = NDVShardSketch.genColumnarHllHint(ec);
        assertThat(hint).contains("ENABLE_MASTER_MPP=true");
    }
}
