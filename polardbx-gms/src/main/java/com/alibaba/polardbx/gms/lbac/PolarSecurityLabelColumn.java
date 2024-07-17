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

package com.alibaba.polardbx.gms.lbac;

/**
 * @author pangzhaoxing
 */
public class PolarSecurityLabelColumn {

    public static final String COLUMN_NAME = "_polar_security_label";

    public static String COLUMN_TYPE = "varchar";

    public static boolean checkPSLName(String name) {
        return COLUMN_NAME.equalsIgnoreCase(name);
    }

    public static boolean checkPSLType(String type) {
        return COLUMN_TYPE.equalsIgnoreCase(type);
    }

}
