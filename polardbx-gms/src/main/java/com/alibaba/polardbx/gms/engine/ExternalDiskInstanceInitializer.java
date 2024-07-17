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

package com.alibaba.polardbx.gms.engine;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.oss.filesystem.cache.CacheManager;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCacheManager;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCachingFileSystem;
import com.alibaba.polardbx.common.properties.FileConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;

public class ExternalDiskInstanceInitializer {

    public String uri;
    public CachePolicy cachePolicy;

    public ExternalDiskInstanceInitializer() {

    }

    public static ExternalDiskInstanceInitializer newBuilder() {
        return new ExternalDiskInstanceInitializer();
    }

    public ExternalDiskInstanceInitializer uri(String uri) {
        this.uri = uri;
        return this;
    }

    public ExternalDiskInstanceInitializer cachePolicy(CachePolicy cachePolicy) {
        this.cachePolicy = cachePolicy;
        return this;
    }

    public FileSystem initialize() {
        Configuration configuration = new Configuration();
        configuration.setBoolean("fs.file.impl.disable.cache", true);

        CacheManager cacheManager = null;
        FileSystem externalFileSystem = null;
        try {
            cacheManager = FileMergeCacheManager.createMergeCacheManager(Engine.EXTERNAL_DISK);
            URI externalUri = URI.create(this.uri);
            Configuration factoryConfig = new Configuration();
            final boolean validationEnabled = FileConfig.getInstance().getCacheConfig().isValidationEnabled();

            externalFileSystem = FileSystem.get(
                externalUri, configuration
            );

            return new FileMergeCachingFileSystem(
                externalUri,
                factoryConfig,
                cacheManager,
                externalFileSystem,
                validationEnabled,
                true
            );
        } catch (Throwable t) {
            if (cacheManager != null) {
                try {
                    cacheManager.close();
                } catch (Throwable t1) {
                    // ignore
                }
            }
            if (externalFileSystem != null) {
                try {
                    externalFileSystem.close();
                } catch (Throwable t1) {
                    // ignore

                }
            }

            throw GeneralUtil.nestedException("Fail to create external disk file system!", t);
        }
    }
}
