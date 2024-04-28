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

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.InstanceRole;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;

public class ConfigDataMode {

    public static final String INSTANCE_ROLE_VARIABLE = "POLARDBX_INSTANCE_ROLE";

    private static InstanceRole instanceRole = InstanceRole.MASTER;

    private static Mode mode;

    public static InstanceRole getInstanceRole() {
        return instanceRole;
    }

    public static void setInstanceRole(int instType) {
        String fastMode = StringUtils.isNotEmpty(System.getProperty("tddl.config.mode")) ?
            System.getProperty("tddl.config.mode") : System.getProperty("configMode");
        if (StringUtils.isNotEmpty(fastMode)) {
            //set the instance role by env.
            if (fastMode.equalsIgnoreCase("FAST_MOCK")) {
                //keep compatible with fast mode.
                ConfigDataMode.instanceRole = InstanceRole.FAST_MOCK;
                return;
            } else if (fastMode.equalsIgnoreCase("COLUMNAR_SLAVE")) {
                ConfigDataMode.instanceRole = InstanceRole.COLUMNAR_SLAVE;
                return;
            }
        }

        if (instType == 0 || instType == 3) {
            ConfigDataMode.instanceRole = InstanceRole.MASTER;
        } else if (instType == 4) {
            ConfigDataMode.instanceRole = InstanceRole.COLUMNAR_SLAVE;
        } else {
            ConfigDataMode.instanceRole = InstanceRole.ROW_SLAVE;
        }
    }

    @VisibleForTesting
    public static void setInstanceRole(InstanceRole instanceRole) {
        ConfigDataMode.instanceRole = instanceRole;
    }

    /**
     * the default mode is GMS, and the MOCK is visible for the Planner UT.
     */
    public enum Mode {
        MOCK("mock"),
        GMS("gms");

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

    public enum LearnerMode {
        ONLY_READ("ONLY_READ"),
        ALLOW_INIT_DML("ALLOW_INIT_DML"),
        ALLOW_USE_DML("ALLOW_USE_DML");

        private String name;

        LearnerMode(String name) {
            this.name = name;
        }

        public static LearnerMode nameOf(String m) {
            for (LearnerMode mode : LearnerMode.values()) {
                if (StringUtils.equalsIgnoreCase(mode.name(), m)) {
                    return mode;
                }
            }

            return null;
        }

        public String getName() {
            return name;
        }

    }

    public static Mode getMode() {
        return mode;
    }

    @VisibleForTesting
    public static void setMode(Mode mode) {
        ConfigDataMode.mode = mode;
    }

    /**
     * 是否为mock模式，主要用于测试
     */
    public static boolean isMock() {
        return mode != null && mode == Mode.MOCK;
    }

    // ========= The DB type of Server =========
    public static boolean isPolarDbX() {
        if (isFastMock() || isMock()) {
            return false;
        } else {
            return mode == Mode.GMS;
        }
    }

    // ========= The instance role of Server =========
    // Check master for all DB type
    public static boolean isMasterMode() {
        return getInstanceRole() == InstanceRole.MASTER || (
            getInstanceRole() == InstanceRole.ROW_SLAVE &&
                DynamicConfig.getInstance().learnerMode().compareTo(LearnerMode.ALLOW_INIT_DML) > 0);
    }

    public static boolean isRowSlaveMode() {
        return getInstanceRole() == InstanceRole.ROW_SLAVE && (
            DynamicConfig.getInstance().learnerMode().compareTo(LearnerMode.ALLOW_USE_DML) < 0);
    }

    public static boolean isColumnarMode() {
        return getInstanceRole() == InstanceRole.COLUMNAR_SLAVE;
    }

    public static boolean isFastMock() {
        return getInstanceRole() == InstanceRole.FAST_MOCK;
    }

    public static boolean needInitMasterModeResource() {
        return getInstanceRole() == InstanceRole.MASTER || (
            getInstanceRole() == InstanceRole.ROW_SLAVE &&
                DynamicConfig.getInstance().learnerMode().compareTo(LearnerMode.ONLY_READ) > 0);
    }

    public static boolean needDNResource() {
        return getInstanceRole() == InstanceRole.MASTER ||
            getInstanceRole() == InstanceRole.ROW_SLAVE;
    }

    public static boolean needGMSResource() {
        return getInstanceRole() != InstanceRole.FAST_MOCK;
    }

    public static boolean isReadOnlyMode() {
        return getInstanceRole() == InstanceRole.COLUMNAR_SLAVE ||
            getInstanceRole() == InstanceRole.ROW_SLAVE;
    }

}
