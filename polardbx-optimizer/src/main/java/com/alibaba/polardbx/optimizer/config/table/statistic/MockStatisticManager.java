package com.alibaba.polardbx.optimizer.config.table.statistic;

import com.alibaba.polardbx.optimizer.config.table.statistic.inf.NDVSketchService;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableColumnStatistic;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableNDVSketchStatistic;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableTableStatistic;

import java.util.Map;

public class MockStatisticManager extends StatisticManager{
    public MockStatisticManager(String schemaName,
                                StatisticDataSource sds) {
        super(schemaName, sds);
    }
//    public MockStatisticManager(String schemaName,
//                                SystemTableTableStatistic systemTableTableStatistic,
//                                SystemTableColumnStatistic systemTableColumnStatistic,
//                                SystemTableNDVSketchStatistic ndvSketchStatistic,
//                                NDVSketchService ndvSketch,
//                                Map<String, Object> connectionProperties) {
//        super(schemaName, systemTableTableStatistic, systemTableColumnStatistic, ndvSketchStatistic, ndvSketch,
//            connectionProperties);
//    }
}
