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

package com.alibaba.polardbx.config;

import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.polardbx.common.utils.InstanceRole;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;


public class ConfigDataMode {

    public static final String CONFIG_MODE = "tddl.config.mode";


    private static Mode mode;


    private static Mode configServerMode;

    private static boolean isQuotaEscape = true;
    private static volatile long refreshConfigTimestamp = 0;
    private static boolean supportRuleParameterNullValue = false;
    private static String atomAddressMode = null;
    private static boolean zeroDataTimeToString = false;

    private static boolean supportSingleDbMultiTbs = false;
    private static boolean supportRemoveDdl = false;
    private static boolean supportDropAutoSeq = false;
    private static boolean allowSimpleSequence = false;

    private static boolean supportSlaveRead = false;

    private static int txIsolation;

    private static String cluster;

    static {
        enableFastJsonAutoType();
        loadConfigDataMode();
    }

    protected static void enableFastJsonAutoType() {

        try {
            ParserConfig.getGlobalInstance().addAccept("com.alibaba.polardbx.");
            ParserConfig.getGlobalInstance().addAccept("org.apache.calcite.");
        } catch (Throwable e) {

        }

    }

    protected static void loadConfigDataMode() {
        if (isFastMock()) {
            return;
        }
        String m = System.getProperty(CONFIG_MODE, "auto");
        mode = Mode.nameOf(m);
        if (mode == null) {
            mode = Mode.AUTO;
        }

        configServerMode = Mode.nameOf(m);
        if (configServerMode == null) {
            configServerMode = Mode.AUTO;
        }

        if (mode != Mode.FAST_MOCK && System.getProperty("metaDbAddr") != null) {
            if (!String.valueOf(System.getProperty("metaDbAddr")).isEmpty()) {
                mode = Mode.GMS;
                configServerMode = Mode.GMS;
            }
        }

        String singleDbMultiTbsSupported = System.getProperty("supportSingleDbMultiTbs");
        supportSingleDbMultiTbs = BooleanUtils.toBoolean(singleDbMultiTbsSupported);

        String removeDdlSupported = System.getProperty("supportRemoveDdl");
            supportRemoveDdl = BooleanUtils.toBoolean(removeDdlSupported);

            String dropAutoSeqSupported = System.getProperty("supportDropAutoSeq");
            supportDropAutoSeq = BooleanUtils.toBoolean(dropAutoSeqSupported);

        String simpleSequenceAllowed = System.getProperty("allowSimpleSequence");
        allowSimpleSequence = BooleanUtils.toBoolean(simpleSequenceAllowed);

        String supportSlaveReaded = System.getProperty("supportSlaveRead");
        supportSlaveRead = BooleanUtils.toBoolean(supportSlaveReaded);
    }

    public static void reload() {
        loadConfigDataMode();
    }

    public enum Mode {
        AUTO(null),
        MOCK("mock"),
        MANAGER("manager"),
        GMS("gms"),
        FAST_MOCK("diamond");

        private String extensionName;

        Mode(String extensionName) {
            this.extensionName = extensionName;
        }

        public static Mode nameOf(String m) {
            for (Mode mode : Mode.values()) {
                if (StringUtils.equalsIgnoreCase(mode.name(), m)) {
                    return mode;
                }
            }

            return null;
        }

        public String getExtensionName() {
            return extensionName;
        }

        public boolean isMock() {
            return this == Mode.MOCK;
        }
    }

    public static Mode getMode() {
        return mode;
    }

    public static void setMode(Mode mode) {
        ConfigDataMode.mode = mode;
    }

    public static Mode getConfigServerMode() {
        return configServerMode;
    }

    public static void setConfigServerMode(Mode mode) {
        configServerMode = mode;
    }

    public static boolean isMock() {
        return mode != null && mode == Mode.MOCK;
    }

    public static boolean isFastMock() {
        return mode != null && mode == Mode.FAST_MOCK;
    }

    public static boolean isMasterMode() {
        return InstanceRoleManager.INSTANCE.getInstanceRole() == InstanceRole.MASTER;
    }

    public static boolean isSlaveMode() {
        return InstanceRoleManager.INSTANCE.getInstanceRole() == InstanceRole.LEARNER;
    }

    public static long getRefreshConfigTimestamp() {
        return refreshConfigTimestamp;
    }

    public static void setRefreshConfigTimestamp(long refreshConfigTimestamp) {
        ConfigDataMode.refreshConfigTimestamp = refreshConfigTimestamp;
    }

    public static boolean isSupportRuleParameterNullValue() {
        return supportRuleParameterNullValue;
    }

    public static void setSupportRuleParameterNullValue(boolean supportRuleParameterNullValue) {
        ConfigDataMode.supportRuleParameterNullValue = supportRuleParameterNullValue;
    }

    public static boolean isQuotaEscape() {
        return isQuotaEscape;
    }

    public static void setQuotaEscape(boolean isQuotaEscape) {
        ConfigDataMode.isQuotaEscape = isQuotaEscape;
    }

    public static boolean isZeroDataTimeToString() {
        return zeroDataTimeToString;
    }

    public static void setZeroDataTimeToString(boolean zeroDataTimeToString) {
        ConfigDataMode.zeroDataTimeToString = zeroDataTimeToString;
    }

    public static String getAtomAddressMode() {
        return atomAddressMode;
    }

    public static void setAtomAddressMode(String atomAddressMode) {
        ConfigDataMode.atomAddressMode = atomAddressMode;
    }

    public static String getCluster() {
        return cluster;
    }

    public static void setCluster(String cluster) {
        ConfigDataMode.cluster = cluster;
    }

    public static boolean isSupportSingleDbMultiTbs() {
        return supportSingleDbMultiTbs;
    }

    public static void setSupportSingleDbMultiTbs(boolean supportSingleDbMultiTbs) {
        ConfigDataMode.supportSingleDbMultiTbs = supportSingleDbMultiTbs;
    }

    public static boolean isSupportRemoveDdl() {
        return supportRemoveDdl;
    }

    public static void setSupportRemoveDdl(boolean supportRemoveDdl) {
        ConfigDataMode.supportRemoveDdl = supportRemoveDdl;
    }

    public static boolean isSupportDropAutoSeq() {
        return supportDropAutoSeq;
    }

    public static void setSupportDropAutoSeq(boolean supportDropAutoSeq) {
        ConfigDataMode.supportDropAutoSeq = supportDropAutoSeq;
    }

    public static boolean isAllowSimpleSequence() {
        return allowSimpleSequence;
    }

    public static void setAllowSimpleSequence(boolean allowSimpleSequence) {
        ConfigDataMode.allowSimpleSequence = allowSimpleSequence;
    }

    public static int getTxIsolation() {
        return txIsolation;
    }

    public static void setTxIsolation(int txIsolation) {
        ConfigDataMode.txIsolation = txIsolation;
    }

    public static boolean enableSlaveReadForPolarDbX() {
        return supportSlaveRead && isMasterMode();
    }
}
