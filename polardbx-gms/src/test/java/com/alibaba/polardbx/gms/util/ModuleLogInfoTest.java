package com.alibaba.polardbx.gms.util;

import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.module.LogLevel;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import org.junit.Assert;
import org.junit.Test;

import static com.alibaba.polardbx.gms.module.LogPattern.START_OVER;

/**
 * @author fangwu
 */
public class ModuleLogInfoTest {
    @Test
    public void testSizeOverflow() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        for (int i = 0; i < 10000; i++) {
            System.out.println(i);
            ModuleLogInfo.getInstance()
                .logRecord(Module.STATISTICS, START_OVER, new String[] {"test", "test"}, LogLevel.NORMAL);
        }
        System.out.println(ModuleLogInfo.getInstance().resources());
        Assert.assertTrue("STATISTICS:1000;".equalsIgnoreCase(ModuleLogInfo.getInstance().resources()));
    }
}
