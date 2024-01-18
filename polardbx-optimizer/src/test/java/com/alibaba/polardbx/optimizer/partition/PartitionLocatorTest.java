/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.partition;

import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundValueKind;
import com.alibaba.polardbx.optimizer.partition.boundspec.RangeBoundSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
@Ignore
public class PartitionLocatorTest {

    @Test
    public void testPartitionLocator() {

        List<Long> boundValList = new ArrayList<>();
        int boundCnt = 100;
        for (int i = 0; i < boundCnt; i++) {
            boundValList.add(new Long((i + 1) * 100));
        }

        String dbName = "test_db";
        String tbName = "test_tb";
        int groupCnt = 32;
        int[] groupStat = new int[groupCnt];
        for (int i = 0; i < groupCnt; i++) {
            groupStat[i] = 0;
        }
        RelDataTypeFactory typeFactory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
        PartitionLocator locator = new PartitionLocator(groupCnt);
        List<PartitionSpec> partitionSpecs = new ArrayList<>();
        for (int i = 0; i < boundValList.size(); i++) {
            PartitionSpec p = new PartitionSpec();
            RangeBoundSpec pBoundSpec = new RangeBoundSpec();
            pBoundSpec.setStrategy(PartitionStrategy.RANGE);
            ;
            Long shardKeyObj = boundValList.get(i);
            PartitionBoundVal pBoundVal =
                PartitionBoundVal.createPartitionBoundVal(null, PartitionBoundValueKind.DATUM_NORMAL_VALUE);
            pBoundSpec.setBoundValue(pBoundVal);
            p.setMaxValueRange(false);
            p.setBoundSpec(pBoundSpec);
            ;
            p.setPosition(Long.valueOf(i + i));
            p.setName(String.format("p%s", i + 1));
            p.setComment("");
            PartitionLocation location = locator.computeLocation(dbName, tbName, p, "test_group");
            p.setLocation(location);
            p.setBoundSpaceComparator(null);
            partitionSpecs.add(p);
        }

        for (int i = 0; i < groupCnt; i++) {
            System.out.println(groupStat[i]);
        }
    }
}
