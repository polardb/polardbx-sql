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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.executor.mpp.client;

import com.google.common.net.MediaType;

public abstract class MppMediaTypes {

    public static final String MPP_POLARDBX = "polardbx";
    public static final String MPP_USER = "X-Mpp-User";

    public static final String MPP_CURRENT_STATE = "X-Mpp-Current-State";
    public static final String MPP_MAX_WAIT = "X-Mpp-Max-Wait";
    public static final String MPP_MAX_SIZE = "X-Mpp-Max-Size";
    public static final String MPP_TASK_INSTANCE_ID = "X-Mpp-Task-Instance-Id";
    public static final String MPP_PAGE_TOKEN = "X-Mpp-Page-Sequence-Id";
    public static final String MPP_PAGE_NEXT_TOKEN = "X-Mpp-Page-End-Sequence-Id";
    public static final String MPP_BUFFER_COMPLETE = "X-Mpp-Buffer-Complete";
    public static final String MPP_PAGES = "application/X-mpp-pages";
    public static final MediaType MPP_PAGES_TYPE = MediaType.create("application", "X-mpp-pages");
}
