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

package com.alibaba.polardbx.common.privilege;

public interface PrivilegeErrorMsg {

    String CREATE_OR_DROP_SYSTEM_ACCOUNT_NOT_ALLOWED = "Creating or dropping system account %s is not allowed.";

    String MODIFY_SYSTEM_ACCOUNT_PRIVILEGE_NOT_ALLOWED = "Changes to system account %s privileges are not allowed.";

    String AUTHORIZE_RESTRICTED_TO_ADMINISTRATOR = "The authorization operations is restricted to administrator only.";

    String AUTHORIZE_GRANT_OPTION_NOT_ALLOWED = "Authorizing 'GRANT OPTION' is not allowed.";

    String GLOBAL_AUTHORIZE_NOT_ALLOWED = "Global authorization is not allowed.";

    String AUTHORIZE_RESTRICTED_TO_PRIVATE_INSTANCE = "The authorization operations are available for dedicated instance only.";

    String PRIVILEGE_SQL_ONLY_OPEN_TO_DRDS = "Privileges and authorization are only available in DRDS.";

    String AUTHORIZE_RESTRICTED_TO_MASTER_INSTANCE     = "The authorization operations are available for master instance only.";
}
