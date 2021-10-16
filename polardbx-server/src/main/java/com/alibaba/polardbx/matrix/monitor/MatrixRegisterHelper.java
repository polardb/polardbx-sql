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

package com.alibaba.polardbx.matrix.monitor;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TDataSource的注册管理器， 管理所有已经成功初始化的TDataSource的应用
 *
 * @author chenghui.lch 2016年6月6日 下午3:29:37
 * @since 5.0.0
 */
public class MatrixRegisterHelper {

    protected final static Logger logger = LoggerFactory.getLogger(MatrixRegisterHelper.class);

    /**
     * key: appName, val: TDataSource
     */
    protected static Map<String, TDataSource> matrixDataSourceMaps = new ConcurrentHashMap<String, TDataSource>();

    public static Map<String, TDataSource> getAllMatrixInfos() {
        return matrixDataSourceMaps;
    }

    public static void registerMatrix(TDataSource matrix) {
        synchronized (matrixDataSourceMaps) {
            String appName = matrix.getAppName();
            matrixDataSourceMaps.put(appName, matrix);
            logger.debug(String.format("register successfully for tdataSource[%s] ", matrix.getAppName()));
        }
    }

    public static void unRegisterMatrix(TDataSource matrix) {
        synchronized (matrixDataSourceMaps) {
            String appName = matrix.getAppName();
            matrixDataSourceMaps.remove(appName);
            logger.debug(String.format("unregister successfully for tdataSource[%s] ", matrix.getAppName()));
        }
    }
}
