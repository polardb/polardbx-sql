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

package com.alibaba.polardbx.atom.config;

import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.apache.commons.lang.BooleanUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * TAtom数据源的推送配置解析类
 *
 * @author qihao
 */
public class TAtomConfParser {

    private static final Logger logger = LoggerFactory.getLogger(TAtomConfParser.class);

    public static final String GLOBA_IP_KEY = "ip";
    public static final String GLOBA_PORT_KEY = "port";
    public static final String GLOBA_DB_NAME_KEY = "dbName";
    public static final String GLOBA_DB_STATUS_KEY = "dbStatus";
    public static final String APP_USER_NAME_KEY = "userName";
    public static final String APP_INIT_POOL_SIZE_KEY = "initPoolSize";
    public static final String APP_PREFILL = "prefill";

    public static final String APP_MIN_POOL_SIZE_KEY = "minPoolSize";
    public static final String APP_MAX_POOL_SIZE_KEY = "maxPoolSize";
    public static final String APP_IDLE_TIMEOUT_KEY = "idleTimeout";
    public static final String APP_BLOCKING_TIMEOUT_KEY = "blockingTimeout";
    public static final String APP_SOCKET_TIMEOUT_KEY = "socketTimeout";
    public static final String APP_PREPARED_STATEMENT_CACHE_SIZE_KEY = "preparedStatementCacheSize";
    public static final String APP_CON_PROP_KEY = "connectionProperties";
    public static final String APP_DESTROY_SCHEDULER = "destroyScheduler";
    public static final String APP_CREATE_SCHEDULER = "createScheduler";

    public static final String APP_DRIVER_CLASS_KEY = "driverClass";

    public static final String APP_CONNECTION_INIT_SQL_KEY = "connectionInitSql";
    public static final String APP_MAX_WAIT_THREAD_COUNT = "maxWaitThreadCount";

    public static final String APP_CONNECTION_EVICTIONTIMEOUT = "evictionTimeout";
    public static final String APP_ON_FATAL_ERROR_MAX_ACTIVE = "onFatalErrorMaxActive";

    /**
     * 写，次数限制
     */
    public static final String APP_WRITE_RESTRICT_TIMES = "writeRestrictTimes";
    /**
     * 读，次数限制
     */
    public static final String APP_READ_RESTRICT_TIMES = "readRestrictTimes";
    /**
     * thread count 次数限制
     */
    public static final String APP_THREAD_COUNT_RESTRICT = "threadCountRestrict";

    public static final String APP_TIME_SLICE_IN_MILLS = "timeSliceInMillis";

    /**
     * 应用连接限制: 限制某个应用键值的并发连接数。
     */
    public static final String APP_CONN_RESTRICT = "connRestrict";

    /**
     * 是否打开纯KV查询的开关
     */
    public static final String APP_DS_MODE = "dsMode";
    /**
     * 是否严格保持物理库连接池的min idle
     */
    public static final String APP_STRICT_KEEP_ALIVE = "strictKeepAlive";

    /**
     * 是否打开Druid的KeepAlive功能
     */
    public static final String APP_ENABLE_KEEP_ALIVE = "enableKeepAlive";

    public static TAtomDsConfDO parserTAtomDsConfDO(String globalConfStr, String appConfStr) {
        TAtomDsConfDO pasObj = new TAtomDsConfDO();
        if (TStringUtil.isNotBlank(globalConfStr)) {
            Properties globaProp = TAtomConfParser.parserConfStr2Properties(globalConfStr);
            if (!globaProp.isEmpty()) {
                String ipKey = TAtomConfParser.GLOBA_IP_KEY;
                String ip = TStringUtil.trim(globaProp.getProperty(ipKey));
                if (TStringUtil.isNotBlank(ip)) {
                    pasObj.setIp(ip);
                }

                String portKey = TAtomConfParser.GLOBA_PORT_KEY;
                String port = TStringUtil.trim(globaProp.getProperty(portKey));
                if (TStringUtil.isNotBlank(port)) {
                    pasObj.setPort(port);
                }
                String dbName = TStringUtil.trim(globaProp.getProperty(TAtomConfParser.GLOBA_DB_NAME_KEY));
                if (TStringUtil.isNotBlank(dbName)) {
                    pasObj.setDbName(dbName);
                }
                String dbStatus = TStringUtil.trim(globaProp.getProperty(TAtomConfParser.GLOBA_DB_STATUS_KEY));
                if (TStringUtil.isNotBlank(dbStatus)) {
                    pasObj.setDbStatus(dbStatus);
                }
            }
        }
        if (TStringUtil.isNotBlank(appConfStr)) {
            Properties appProp = TAtomConfParser.parserConfStr2Properties(appConfStr);
            if (!appProp.isEmpty()) {
                String userName = TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_USER_NAME_KEY));
                if (TStringUtil.isNotBlank(userName)) {
                    pasObj.setUserName(userName);
                }
                String initPoolSize = TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_INIT_POOL_SIZE_KEY));
                if (TStringUtil.isNotBlank(initPoolSize) && TStringUtil.isNumeric(initPoolSize)) {
                    pasObj.setInitPoolSize(Integer.valueOf(initPoolSize));
                }
                String minPoolSize = TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_MIN_POOL_SIZE_KEY));
                if (TStringUtil.isNotBlank(minPoolSize) && TStringUtil.isNumeric(minPoolSize)) {
                    pasObj.setMinPoolSize(Integer.valueOf(minPoolSize));
                }
                String maxPoolSize = TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_MAX_POOL_SIZE_KEY));
                if (TStringUtil.isNotBlank(maxPoolSize) && TStringUtil.isNumeric(maxPoolSize)) {
                    pasObj.setMaxPoolSize(Integer.valueOf(maxPoolSize));
                }
                if (MppConfig.getInstance().getTableScanDsMaxSize() > pasObj.getMaxPoolSize()) {
                    pasObj.setMaxPoolSize(MppConfig.getInstance().getTableScanDsMaxSize());
                }

                String maxWaitThreadCount =
                    TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_MAX_WAIT_THREAD_COUNT));
                if (TStringUtil.isNotBlank(maxWaitThreadCount) && TStringUtil.isNumeric(maxWaitThreadCount)) {
                    pasObj.setMaxWaitThreadCount(Integer.valueOf(maxWaitThreadCount));
                }
                String idleTimeout = TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_IDLE_TIMEOUT_KEY));
                if (TStringUtil.isNotBlank(idleTimeout) && TStringUtil.isNumeric(idleTimeout)) {
                    pasObj.setIdleTimeout(Long.valueOf(idleTimeout));
                }
                String blockingTimeout =
                    TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_BLOCKING_TIMEOUT_KEY));
                if (TStringUtil.isNotBlank(blockingTimeout) && TStringUtil.isNumeric(blockingTimeout)) {
                    pasObj.setBlockingTimeout(Integer.valueOf(blockingTimeout));
                }
                String preparedStatementCacheSize =
                    TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_PREPARED_STATEMENT_CACHE_SIZE_KEY));
                if (TStringUtil.isNotBlank(preparedStatementCacheSize)
                    && TStringUtil.isNumeric(preparedStatementCacheSize)) {
                    pasObj.setPreparedStatementCacheSize(Integer.valueOf(preparedStatementCacheSize));
                }

                String writeRestrictTimes =
                    TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_WRITE_RESTRICT_TIMES));
                if (TStringUtil.isNotBlank(writeRestrictTimes) && TStringUtil.isNumeric(writeRestrictTimes)) {
                    pasObj.setWriteRestrictTimes(Integer.valueOf(writeRestrictTimes));
                }

                String readRestrictTimes =
                    TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_READ_RESTRICT_TIMES));
                if (TStringUtil.isNotBlank(readRestrictTimes) && TStringUtil.isNumeric(readRestrictTimes)) {
                    pasObj.setReadRestrictTimes(Integer.valueOf(readRestrictTimes));
                }
                String threadCountRestrict =
                    TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_THREAD_COUNT_RESTRICT));
                if (TStringUtil.isNotBlank(threadCountRestrict) && TStringUtil.isNumeric(threadCountRestrict)) {
                    pasObj.setThreadCountRestrict(Integer.valueOf(threadCountRestrict));
                }
                String timeSliceInMillis =
                    TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_TIME_SLICE_IN_MILLS));
                if (TStringUtil.isNotBlank(timeSliceInMillis) && TStringUtil.isNumeric(timeSliceInMillis)) {
                    pasObj.setTimeSliceInMillis(Integer.valueOf(timeSliceInMillis));
                }

                String conPropStr = TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_CON_PROP_KEY));
                Map<String, String> connectionProperties = parserConPropStr2Map(conPropStr);
                if (null != connectionProperties && !connectionProperties.isEmpty()) {
                    pasObj.setConnectionProperties(connectionProperties);
                    String driverClass = connectionProperties.get(TAtomConfParser.APP_DRIVER_CLASS_KEY);
                    if (!TStringUtil.isBlank(driverClass)) {
                        pasObj.setDriverClass(driverClass);
                    }

                    if (connectionProperties.containsKey(APP_PREFILL)) {
                        // add by agapple, 简单处理支持下初始化链接
                        String prefill = connectionProperties.remove(APP_PREFILL);
                        if (BooleanUtils.toBoolean(prefill)
                            && pasObj.getInitPoolSize() == TAtomDsConfDO.defaultInitPoolSize) {
                            pasObj.setInitPoolSize(pasObj.getMinPoolSize());
                        }
                    }

                    if (connectionProperties.containsKey(APP_DS_MODE)) {
                        String appDsMode = connectionProperties.remove(APP_DS_MODE);
                        if (!TStringUtil.isBlank(appDsMode)) {
                            pasObj.setDsMode(appDsMode);
                        }
                    }

                    String isStrictKeepAlive = null;
                    isStrictKeepAlive = TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_STRICT_KEEP_ALIVE));
                    if (TStringUtil.isNotBlank(isStrictKeepAlive)) {
                        pasObj.setStrictKeepAlive(Boolean.valueOf(isStrictKeepAlive));
                    }
                    if (connectionProperties.containsKey(APP_STRICT_KEEP_ALIVE)) {
                        isStrictKeepAlive = connectionProperties.remove(APP_STRICT_KEEP_ALIVE);
                        if (TStringUtil.isNotBlank(isStrictKeepAlive)) {
                            pasObj.setStrictKeepAlive(Boolean.valueOf(isStrictKeepAlive));
                        }
                    }

                    if (connectionProperties.containsKey(APP_ENABLE_KEEP_ALIVE)) {
                        String enableKeepAlive = connectionProperties.remove(APP_ENABLE_KEEP_ALIVE);
                        if (TStringUtil.isNotBlank(enableKeepAlive)) {
                            pasObj.setEnableKeepAlive(Boolean.valueOf(enableKeepAlive));
                        }
                    }

                    if (connectionProperties.containsKey(APP_CREATE_SCHEDULER)) {
                        if (!BooleanUtils.toBoolean(connectionProperties.remove(APP_CREATE_SCHEDULER))) {
                            pasObj.setCreateScheduler(false);
                        }
                    }

                    if (connectionProperties.containsKey(APP_DESTROY_SCHEDULER)) {
                        if (!BooleanUtils.toBoolean(connectionProperties.remove(APP_DESTROY_SCHEDULER))) {
                            pasObj.setDestroyScheduler(false);
                        }
                    }

                    String connectionInitSql = connectionProperties.remove(TAtomConfParser.APP_CONNECTION_INIT_SQL_KEY);
                    if (!TStringUtil.isBlank(connectionInitSql)) {
                        pasObj.setConnectionInitSql(connectionInitSql);
                    }

                    maxWaitThreadCount = connectionProperties.remove(TAtomConfParser.APP_MAX_WAIT_THREAD_COUNT);
                    if (!TStringUtil.isBlank(maxWaitThreadCount)) {
                        pasObj.setMaxWaitThreadCount(Integer.valueOf(maxWaitThreadCount));
                    }

                    String evictionTimeout =
                        connectionProperties.remove(TAtomConfParser.APP_CONNECTION_EVICTIONTIMEOUT);
                    if (!TStringUtil.isBlank(evictionTimeout)) {
                        pasObj.setEvictionTimeout(Integer.valueOf(evictionTimeout));
                    }

                    String onFatalErrorMaxActive =
                        connectionProperties.remove(TAtomConfParser.APP_ON_FATAL_ERROR_MAX_ACTIVE);
                    if (!TStringUtil.isBlank(onFatalErrorMaxActive)) {
                        pasObj.setOnFatalErrorMaxActive(Integer.valueOf(onFatalErrorMaxActive));
                    }
                }
            }
        }
        return pasObj;
    }

    public static Map<String, String> parserConPropStr2Map(String conPropStr) {
        Map<String, String> connectionProperties = null;
        if (TStringUtil.isNotBlank(conPropStr)) {
            String[] keyValues = TStringUtil.split(conPropStr, ";");
            if (null != keyValues && keyValues.length > 0) {
                connectionProperties = new HashMap<String, String>(keyValues.length);
                for (String keyValue : keyValues) {
                    String key = TStringUtil.substringBefore(keyValue, "=");
                    String value = TStringUtil.substringAfter(keyValue, "=");
                    if (TStringUtil.isNotBlank(key) && TStringUtil.isNotBlank(value)) {
                        connectionProperties.put(key.trim(), value.trim());
                    }
                }
            }
        }
        return connectionProperties;
    }

    public static Properties parserConfStr2Properties(String data) {
        Properties prop = new Properties();
        if (TStringUtil.isNotBlank(data)) {
            ByteArrayInputStream byteArrayInputStream = null;
            try {
                byteArrayInputStream = new ByteArrayInputStream((data).getBytes());
                prop.load(byteArrayInputStream);
            } catch (IOException e) {
                logger.error("parserConfStr2Properties Error", e);
            } finally {
                try {
                    if (byteArrayInputStream != null) {
                        byteArrayInputStream.close();
                    }
                } catch (IOException e) {
                    logger.error("parserConfStr2Properties Error", e);
                }
            }
        }
        return prop;
    }

    /**
     * HASH 策略的最大槽数量限制。
     */
    public static final int MAX_HASH_RESTRICT_SLOT = 32;

}
