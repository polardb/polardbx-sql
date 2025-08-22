package com.alibaba.polardbx.gms.metadb.cdc;

import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

public class BinlogDumpAccessorTest {

    @Test
    public void testGetDumperMaster() {
        MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = Mockito.mockStatic(MetaDbUtil.class);
        MockedStatic<InstIdUtil> instIdUtilMockedStatic = Mockito.mockStatic(InstIdUtil.class);
        MockedStatic<MetaDbLogUtil> metaDbLogUtilMockedStatic = Mockito.mockStatic(MetaDbLogUtil.class);
        // 设置mock行为
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.hasColumn(anyString(), anyString())).thenReturn(false);

        Connection mockConnection = mock(Connection.class);
        metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConnection);

        List<BinlogDumperRecord> result = new ArrayList<>();
        result.add(new BinlogDumperRecord());
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(
                anyString(),
                eq(BinlogDumperRecord.class),
                any()))
            .thenReturn(result);

        // 执行测试
        BinlogDumperAccessor accessor = new BinlogDumperAccessor();
        Optional<BinlogDumperRecord> actual = accessor.getDumperMaster();

        // 验证结果
        assertTrue(actual.isPresent());

        metaDbUtilMockedStatic.close();
        instIdUtilMockedStatic.close();
        metaDbLogUtilMockedStatic.close();
    }
}
