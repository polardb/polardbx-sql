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

package com.alibaba.polardbx.atom.config.gms;

import com.alibaba.polardbx.atom.config.TAtomConfParser;
import com.alibaba.polardbx.atom.config.TAtomDsConfDO;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;
import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.config.impl.ConnPoolConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.GmsJdbcUtil;
import com.alibaba.polardbx.gms.util.PasswdUtil;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

/**
 * @author chenghui.lch
 */
public class TAtomDsGmsConfigHelper {

    public static final String ATOM_APP_CONF_TEMPLATE =
        "userName=%s\nminPoolSize=%s\nmaxPoolSize=%s\nmaxWaitThreadCount=%s\nidleTimeout=%s\nblockingTimeout=%s\nconnectionProperties=%s\n";

    public static final String ATOM_GLOBAL_CONF_TEMPLATE =
        "ip=%s\nport=%s\ndbName=%s\ndbType=%s\ndbStatus=%s\n";

    protected static final String CHARACTER_ENCODING = "characterEncoding";
    protected static final String CONN_INIT_SQL = "connectionInitSql";
    protected static final String CHARACTER_UTF8MB4 = "utf8mb4";
    protected static final String CHARACTER_UTF8 = "utf8";

    public static TAtomDsConfDO buildAtomDsConfByGms(String storageNodeAddr,
                                                     int xport,
                                                     String userName,
                                                     String passwdEnc,
                                                     String phyDbName,
                                                     ConnPoolConfig storageInstConfig,
                                                     String schemaName) {
        Pair<String, Integer> ipPortPair = AddressUtils.getIpPortPairByAddrStr(storageNodeAddr);

        String ip = ipPortPair.getKey();
        String port = String.valueOf(ipPortPair.getValue());
        String passwd = PasswdUtil.decrypt(passwdEnc);
        String dbName = phyDbName;
        String dbType = "mysql";
        String dbStatus = "RW";

        String minPoolSize = String.valueOf(storageInstConfig.minPoolSize);
        String maxPoolSize = String.valueOf(storageInstConfig.maxPoolSize);
        String maxWaitThreadCount = String.valueOf(storageInstConfig.maxWaitThreadCount);
        String idleTimeout = String.valueOf(storageInstConfig.idleTimeout);
        String blockingTimeout = String.valueOf(storageInstConfig.blockTimeout);
        String connectionPropertiesOfStorage = storageInstConfig.connProps;
        String connectionProperties = connectionPropertiesOfStorage;
        String dbCharset = DbInfoManager.getInstance().getDbChartSet(schemaName);
        if (!StringUtils.isEmpty(dbCharset)) {
            connectionProperties = replaceCharsetEncodingAndAddInitSql(connectionProperties, dbCharset);
        }

        String globalDataVal = String.format(ATOM_GLOBAL_CONF_TEMPLATE, ip, port, dbName, dbType, dbStatus);
        String appDataVal =
            String.format(ATOM_APP_CONF_TEMPLATE, userName, minPoolSize, maxPoolSize, maxWaitThreadCount, idleTimeout,
                blockingTimeout,
                connectionProperties);
        TAtomDsConfDO atomDsConf = TAtomConfParser.parserTAtomDsConfDO(globalDataVal, appDataVal);
        atomDsConf.setPasswd(passwd);

        // Record valid xport.
        atomDsConf.setOriginXport(xport);
        // Note this is Xproto for **STORAGE** node. First check global setting then use the metaDB inst_config.
        if (XConnectionManager.getInstance().getStorageDbPort() != 0) {
            xport = XConnectionManager.getInstance().getStorageDbPort(); // Disabled or force set by server.properties.
        } else if (storageInstConfig.xprotoStorageDbPort != null) {
            if (storageInstConfig.xprotoStorageDbPort != 0) {
                xport = storageInstConfig.xprotoStorageDbPort; // Disabled or force set by inst_config.
            } // else auto set by HA.
        } else {
            // Bad config? Disable it.
            xport = -1;
        }
        atomDsConf.setXport(xport);

        return atomDsConf;
    }

    private static String replaceCharsetEncodingAndAddInitSql(String atomConnProps, String dbDefaultCharset) {
        Map<String, String> connPropsMap = GmsJdbcUtil.getPropertiesMapFromAtomConnProps(atomConnProps);
        if (dbDefaultCharset.equalsIgnoreCase(CHARACTER_UTF8MB4)) {
            connPropsMap.put(CHARACTER_ENCODING, CHARACTER_UTF8);
            String initSql = String.format("set names %s", CHARACTER_UTF8MB4);
            connPropsMap.put(CONN_INIT_SQL, initSql);
        } else {
            connPropsMap.put(CHARACTER_ENCODING, dbDefaultCharset);
        }
        String jdbcConnProps = GmsJdbcUtil.getAtomConnPropsFromPropertiesMap(connPropsMap);
        return jdbcConnProps;
    }
}
