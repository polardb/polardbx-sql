package com.alibaba.polardbx.executor.ddl.job.task;

import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlIntraTaskExecutor;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlTaskSqlBuilder;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.net.util.ColumnarTargetUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * @author chenghui.lch
 */
public class TtlIntraTaskExecutorTest {

    @Test
    public void testInitIntraTaskExecutor() {
        try {

            StorageHaManager mockStorageHaManager = mock(StorageHaManager.class);
            try (MockedStatic<StorageHaManager> storageHaManagerMockedStatic = mockStatic(StorageHaManager.class)) {

                storageHaManagerMockedStatic.when(StorageHaManager::getInstance).thenReturn(mockStorageHaManager);
                doAnswer(invocation -> {
                    // 方法体内没有任何操作，相当于没有返回值
                    return null; // void方法返回null
                }).when(mockStorageHaManager).registerStorageInfoSubListener(any());

                TtlIntraTaskExecutor ttlIntraTaskExecutor = TtlIntraTaskExecutor.getInstance();
                int selectCnt = TtlConfigUtil.getTtlGlobalSelectWorkerCount();
                int selectCoreSize = ttlIntraTaskExecutor.getSelectTaskExecutor().getCorePoolSize();
                Assert.assertTrue(selectCnt == selectCoreSize);

                int deleteCoreSize = ttlIntraTaskExecutor.getDeleteTaskExecutor().getCorePoolSize();
                Assert.assertTrue(4 == deleteCoreSize);

            }

        } catch (Throwable ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }
}
