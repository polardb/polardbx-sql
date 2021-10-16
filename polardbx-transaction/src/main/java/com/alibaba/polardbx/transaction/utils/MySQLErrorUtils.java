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

package com.alibaba.polardbx.transaction.utils;

import java.sql.SQLException;

public class MySQLErrorUtils {

    public static boolean isTransient(SQLException ex) {
        switch (ex.getErrorCode()) {
        case 11:      // Resource temporarily unavailable (EAGAIN)
        case 1040:    // Too many connections
        case 1205:    // Lock wait timeout exceeded; try restarting transaction
        case 1213:    // Deadlock found when trying to get lock; try restarting transaction
        case 1614:    // Transaction branch was rolled back: deadlock was detected
        case 2013:    // Lost connection to MySQL server during query
            return true;
        default:
            return false;
        }
    }
}
