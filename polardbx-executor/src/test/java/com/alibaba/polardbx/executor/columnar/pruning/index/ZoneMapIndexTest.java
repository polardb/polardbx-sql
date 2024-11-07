package com.alibaba.polardbx.executor.columnar.pruning.index;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.columnar.pruning.index.builder.ZoneMapIndexBuilder;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import java.util.Random;

/**
 * @author fangwu
 */
public class ZoneMapIndexTest {

    @Test
    public void testZoneMapGroup() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        ZoneMapIndexBuilder zoneMapIndexBuilder = new ZoneMapIndexBuilder();
        zoneMapIndexBuilder.appendColumn(1, DataTypes.IntegerType);
        Random r = new Random();
        for (int i = 0; i < 100000; i++) {
            int start = r.nextInt(2000);
            zoneMapIndexBuilder.appendIntegerData(1, start).appendIntegerData(1, start + 500);
        }

        ZoneMapIndex zoneMapIndexWithGroup = zoneMapIndexBuilder.build();
        MetaDbInstConfigManager.getInstance().getCnVariableConfigMap().put("ZONEMAP_MAX_GROUP_SIZE", "1");
        ZoneMapIndex zoneMapIndexWithoutGroup = zoneMapIndexBuilder.build();

        assert zoneMapIndexWithGroup.groupSize(1) > 0;
        assert zoneMapIndexWithoutGroup.groupSize(1) == 0;

        long rgNum = zoneMapIndexWithGroup.rgNum();

        for (int i = 0; i < 100; i++) {
            int testStart = r.nextInt(2000);
            int range = r.nextInt(500);

            RoaringBitmap rsWithGroup = RoaringBitmap.bitmapOfRange(0, rgNum);
            zoneMapIndexWithGroup.prune(1, testStart, true, testStart + range, true, rsWithGroup);
            RoaringBitmap rsWithoutGroup = RoaringBitmap.bitmapOfRange(0, rgNum);
            zoneMapIndexWithoutGroup.prune(1, testStart, true, testStart + range, true, rsWithoutGroup);

            int cardinality = rsWithGroup.getCardinality();
            assert cardinality == rsWithoutGroup.getCardinality();
            rsWithGroup.and(rsWithoutGroup);
            assert cardinality == rsWithGroup.getCardinality();
        }

        MetaDbInstConfigManager.getInstance().getCnVariableConfigMap().remove("ZONEMAP_MAX_GROUP_SIZE");

    }

    @Test
    public void testInvalidZoneMapGroup() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        ZoneMapIndexBuilder zoneMapIndexBuilder = new ZoneMapIndexBuilder();
        zoneMapIndexBuilder.appendColumn(1, DataTypes.IntegerType);
        Random r = new Random();
        for (int i = 0; i < 100000; i++) {
            int start = r.nextInt(2000);
            zoneMapIndexBuilder.appendIntegerData(1, start).appendIntegerData(1, start + 500);
        }

        ZoneMapIndex zoneMapIndexWithGroup = zoneMapIndexBuilder.build();
        MetaDbInstConfigManager.getInstance().getCnVariableConfigMap().put("ZONEMAP_MAX_GROUP_SIZE", "1");

        long rgNum = zoneMapIndexWithGroup.rgNum();

        for (int i = 0; i < 100; i++) {
            int testStart = r.nextInt(5000);
            int range = r.nextInt(500);
            RoaringBitmap rsWithGroup = RoaringBitmap.bitmapOfRange(0, rgNum);
            zoneMapIndexWithGroup.prune(1, testStart, true, testStart - range, true, rsWithGroup);
            Assert.assertTrue(rsWithGroup.getCardinality() >= 0);
        }

        MetaDbInstConfigManager.getInstance().getCnVariableConfigMap().remove("ZONEMAP_MAX_GROUP_SIZE");

    }

}
