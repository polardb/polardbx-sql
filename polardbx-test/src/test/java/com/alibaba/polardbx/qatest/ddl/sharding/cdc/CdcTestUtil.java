package com.alibaba.polardbx.qatest.ddl.sharding.cdc;

import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.commons.lang3.StringUtils;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.POLARDBX_SERVER_ID_CONF;

/**
 * created by ziyang.lb
 **/
public class CdcTestUtil {

    public static String getServerId4Check(String serverId) {
        if (serverId == null && StringUtils.containsIgnoreCase(PropertiesUtil.getConnectionProperties(),
            POLARDBX_SERVER_ID_CONF)) {
            return "181818";
        }
        return serverId;
    }
}
