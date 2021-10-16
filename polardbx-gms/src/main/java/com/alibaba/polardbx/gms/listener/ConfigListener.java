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

package com.alibaba.polardbx.gms.listener;

/**
 * @author chenghui.lch
 */
public interface ConfigListener {

    /**
     * Listen to config changes of the dataId and process
     * accordingly as soon as a config change occurs.
     *
     * @param dataId a dataId that the caller defines
     * @param newOpVersion new generated operation version
     */
    void onHandleConfig(String dataId, long newOpVersion);

}
