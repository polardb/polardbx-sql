package com.alibaba.polardbx.optimizer.view;

import com.alibaba.polardbx.common.utils.Assert;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */
public class InformationSchemaMetricTest {

    @Test
    public void testRowType(){
        RelOptCluster cluster = mock(RelOptCluster.class);
        RelDataTypeFactory factory = mock(RelDataTypeFactory.class);
        RelDataType varcharMock = mock(RelDataType.class);
        RelDataType resultMock = mock(RelDataType.class);
        when(factory.createSqlType(SqlTypeName.VARCHAR)).thenReturn(varcharMock);
        when(factory.createStructType(anyList())).thenReturn(resultMock);
        when(cluster.getTypeFactory()).thenReturn(factory);

        InformationSchemaMetric metric = new InformationSchemaMetric(cluster, mock(RelTraitSet.class));
        RelDataType relDataType = metric.deriveRowType();

        Assert.assertTrue(relDataType == resultMock);
    }
}
