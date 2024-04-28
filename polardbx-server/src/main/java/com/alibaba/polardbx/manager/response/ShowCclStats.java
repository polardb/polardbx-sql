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

package com.alibaba.polardbx.manager.response;

import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.manager.ManagerConnection;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.util.LongUtil;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import com.alibaba.polardbx.optimizer.ccl.CclManager;
import com.alibaba.polardbx.optimizer.ccl.common.CclRuleInfo;
import com.alibaba.polardbx.optimizer.ccl.service.ICclConfigService;
import com.alibaba.polardbx.optimizer.ccl.service.ICclService;

import java.util.List;
import java.util.Map;

/**
 * @author busu
 * date: 2020/10/28 6:40 下午
 */
public class ShowCclStats {

    private static final int FIELD_COUNT = 10;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("Running", Fields.FIELD_TYPE_STRING);
        fields[i++].packetId = ++packetId;
        fields[i] = PacketUtil.getField("Waiting", Fields.FIELD_TYPE_STRING);
        fields[i++].packetId = ++packetId;
        fields[i] = PacketUtil.getField("Killed", Fields.FIELD_TYPE_STRING);
        fields[i++].packetId = ++packetId;
        fields[i] = PacketUtil.getField("MatchHitCache", Fields.FIELD_TYPE_STRING);
        fields[i++].packetId = ++packetId;
        fields[i] = PacketUtil.getField("TotalMatchCont", Fields.FIELD_TYPE_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("not_Match_Conn_Cache_Hit_Count", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;
        fields[i] = PacketUtil.getField("not_Match_Plan_Param_Cache_Hit_Count", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;
        fields[i] = PacketUtil.getField("not_Match_Plan_Invalid_Hit_Count", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;
        fields[i] = PacketUtil.getField("match_Ccl_Rule_Hit_Count", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;
        fields[i] = PacketUtil.getField("total_Match_Ccl_Rule_Hit_Count", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        eof.packetId = ++packetId;
    }

    public static void execute(ManagerConnection c) {
        ByteBufferHolder buffer = c.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        proxy.packetBegin();

        // write header
        proxy = header.write(proxy);

        // write fields
        for (FieldPacket field : fields) {
            proxy = field.write(proxy);
        }

        // write eof
        proxy = eof.write(proxy);
        byte packetId = eof.packetId;

        ICclConfigService cclConfigService = CclManager.getCclConfigService();
        List<CclRuleInfo> cclRuleInfos = cclConfigService.getCclRuleInfos();
        Map<String, Integer> runningCountMap = Maps.newHashMap();
        Map<String, Integer> waitQueueMap = Maps.newHashMap();
        Map<String, Long> killedCountMap = Maps.newHashMap();
        Map<String, Long> matchHitCache = Maps.newHashMap();
        Map<String, Long> totalMatchCount = Maps.newHashMap();
        for (CclRuleInfo cclRuleInfo : cclRuleInfos) {
            String id = cclRuleInfo.getCclRuleRecord().id;
            runningCountMap.put(id, cclRuleInfo.getRunningCount().get());
            waitQueueMap.put(id, cclRuleInfo.getStayCount().get() - cclRuleInfo.getRunningCount().get());
            killedCountMap.put(id, cclRuleInfo.getCclRuntimeStat().killedCount.get());
            matchHitCache.put(id, cclRuleInfo.getCclRuntimeStat().matchCclRuleHitCount.get());
            totalMatchCount.put(id, cclRuleInfo.getCclRuntimeStat().totalMatchCclRuleCount.get());
        }

        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        String runningCountJsonStr = JSON.toJSONString(runningCountMap);
        row.add(StringUtil.encode(runningCountJsonStr, c.getResultSetCharset()));
        String waitQueueJsonStr = JSON.toJSONString(waitQueueMap);
        row.add(StringUtil.encode(waitQueueJsonStr, c.getResultSetCharset()));
        String killedCountJsonStr = JSON.toJSONString(killedCountMap);
        row.add(StringUtil.encode(killedCountJsonStr, c.getResultSetCharset()));
        String matchCclRulehitCountJsonStr = JSON.toJSONString(matchHitCache);
        row.add(StringUtil.encode(matchCclRulehitCountJsonStr, c.getResultSetCharset()));
        String totalMatchCclRuleCount = JSON.toJSONString(totalMatchCount);
        row.add(StringUtil.encode(totalMatchCclRuleCount, c.getResultSetCharset()));

        ICclService cclService = CclManager.getService();
        List<Long> cclCacheStats = cclService.getCacheStats();
        for (Long value : cclCacheStats) {
            row.add(LongUtil.toBytes(value));
        }

        row.packetId = ++packetId;
        proxy = row.write(proxy);
        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        proxy = lastEof.write(proxy);

        // write buffer
        proxy.packetEnd();

    }

}
