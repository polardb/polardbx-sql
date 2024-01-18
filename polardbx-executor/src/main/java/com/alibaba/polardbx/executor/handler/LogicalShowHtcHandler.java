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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.utils.GCState;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.execution.PriorityExecutorInfo;
import com.alibaba.polardbx.executor.mpp.execution.TaskExecutor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import org.apache.calcite.rel.RelNode;
import org.hyperic.sigar.NetInterfaceConfig;
import org.hyperic.sigar.NetInterfaceStat;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.hyperic.sigar.SigarNotImplementedException;

import java.math.RoundingMode;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author chenmo.cm
 */
public class LogicalShowHtcHandler extends HandlerCommon {

    private static Sigar sigar = new Sigar();

    public LogicalShowHtcHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {

        ArrayResultCursor result = new ArrayResultCursor("HTC");

        result.addColumn("CURRENT_TIME", DataTypes.LongType);// 当前系统时间
        result.addColumn("CPU", DataTypes.DoubleType);// 当前实例时CPU百分比
        result.addColumn("LOAD", DataTypes.DoubleType);// 当前实例时的机器load
        result.addColumn("FREEMEM", DataTypes.DoubleType);// 当前机器内存的剩余内存,单位K

        result.addColumn("NETIN", DataTypes.LongType);// 当前机器网络数据接收量，Byte
        result.addColumn("NETOUT", DataTypes.LongType);// 当前机器网络数据发出量，Byte
        result.addColumn("NETIO", DataTypes.LongType);// 当前机器网络IO，Byte
        result.addColumn("FULLGCCOUNT", DataTypes.LongType);// FULL GC启动以来次数
        result.addColumn("FULLGCTIME", DataTypes.LongType);// FULL GC启动以来耗时

        result.addColumn("DDL_JOB_COUNT", DataTypes.IntegerType); // #DDL-jobs

        List<Object> obs = getHostInfo(executionContext);
        result.addRow(obs.toArray());
        return result;
    }

    public static List<Object> getHostInfo(ExecutionContext executionContext) {
        List<Object> obs = new ArrayList<Object>();
        Map<String, Object> netInfo = getNetInfo();
        String loadStr = "";

        try {
            if (System.getProperty("os.name").toLowerCase().indexOf("windows") == -1) {
                double[] load = sigar.getLoadAverage();
                if (load.length == 3) {
                    loadStr = load[0] + "," + load[1] + "," + load[2];
                }
            } else {
                loadStr = "win not support";
            }
        } catch (SigarException e) {
            e.printStackTrace();
            loadStr = "win not support";
        }

        try {
            obs.add(System.currentTimeMillis());
            obs.add(sigar.getCpuPerc().getCombined());
            obs.add(loadStr);
            obs.add(sigar.getMem().getFreePercent() / 100D);
            obs.add(netInfo.get("netIn"));
            obs.add(netInfo.get("netOut"));
            obs.add(netInfo.get("netIo"));
            obs.add(GCState.getOldGenCollectionCount());
            obs.add(GCState.getOldGenCollectionTime());

            int ddlJobCount = 0;
            obs.add(ddlJobCount);

        } catch (SigarException e) {
            e.printStackTrace();
        }
        return obs;
    }

    public static double getCpuPercCombined() {
        try {
            return sigar.getCpuPerc().getCombined();
        } catch (SigarException e) {
            return 0;
        }
    }

    public static List<Object> getHostInfo4Manager(String schemaName) {
        List<Object> obs = new ArrayList<Object>();
        Map<String, Object> netInfo = getNetInfo();
        String loadStr = "";

        NumberFormat nf = NumberFormat.getNumberInstance();
        nf.setMinimumFractionDigits(2);// 设置保留小数位
        nf.setRoundingMode(RoundingMode.HALF_UP); // 设置舍入模式
        try {
            if (System.getProperty("os.name").toLowerCase().indexOf("windows") == -1) {
                double[] load = sigar.getLoadAverage();
                if (load.length == 3) {
                    loadStr = load[0] + "";
                }
            } else {
                loadStr = "-1";
            }
        } catch (SigarException | UnsatisfiedLinkError e) {
            e.printStackTrace();
            loadStr = "-1";
        }

        double loadNum = Double.valueOf(loadStr);

        try {
            obs.add(System.currentTimeMillis());
            obs.add(sigar.getCpuPerc().getCombined());
            obs.add(loadNum);
            obs.add(sigar.getMem().getFreePercent() / 100D);
            obs.add(netInfo.get("netIn"));
            obs.add(netInfo.get("netOut"));
            obs.add(netInfo.get("netIo"));
            obs.add(GCState.getOldGenCollectionCount());
            obs.add(GCState.getOldGenCollectionTime());
            obs.add(MemoryManager.getInstance().getGlobalMemoryPool().getMemoryUsage());

            int ddlJobCount = 0;
            obs.add(ddlJobCount);

            if (ServiceProvider.getInstance().getServer() != null) {
                TaskExecutor executor = ServiceProvider.getInstance().getServer().getTaskExecutor();
                PriorityExecutorInfo lowPriorityInfo = executor.getLowPriorityInfo();
                PriorityExecutorInfo highPriorityInfo = executor.getHighPriorityInfo();
                if (ServiceProvider.getInstance().getServer().getQueryManager() != null) {
                    obs.add(ServiceProvider.getInstance().getServer().getQueryManager().getTotalQueries());
                } else {
                    obs.add(0);
                }
                obs.add(highPriorityInfo.getRunnerProcessCount());
                obs.add(lowPriorityInfo.getRunnerProcessCount());
                obs.add(highPriorityInfo.getPendingSplitsSize());
                obs.add(lowPriorityInfo.getPendingSplitsSize());
                obs.add(highPriorityInfo.getBlockedSplitSize() + lowPriorityInfo.getBlockedSplitSize());
            } else {
                obs.add(0);
                obs.add(0);
                obs.add(0);
                obs.add(0);
                obs.add(0);
                obs.add(0);
            }
        } catch (SigarException | UnsatisfiedLinkError e) {
            e.printStackTrace();
        }
        return obs;
    }

    private static Map<String, Object> getNetInfo() {
        Map<String, Object> map = new HashMap<String, Object>();
        long netRev = 0L;
        long netSend = 0L;
        String ifNames[] = null;
        try {
            ifNames = sigar.getNetInterfaceList();
            for (int i = 0; i < ifNames.length; i++) {
                String name = ifNames[i];
                NetInterfaceConfig ifconfig = sigar.getNetInterfaceConfig(name);
                if ((ifconfig.getFlags() & 1L) <= 0L) {
                    continue;
                }
                try {
                    NetInterfaceStat ifstat = sigar.getNetInterfaceStat(name);
                    netRev += ifstat.getRxBytes();
                    netSend += ifstat.getTxBytes();
                } catch (SigarException e) {
                    // ignore
                }
            }
        } catch (Exception | UnsatisfiedLinkError e) {
            e.printStackTrace();
        }
        map.put("netIn", netRev);
        map.put("netOut", netSend);
        map.put("netIo", netRev + netSend);
        return map;
    }
}
