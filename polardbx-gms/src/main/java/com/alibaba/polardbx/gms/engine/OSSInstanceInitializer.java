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
import com.alibaba.polardbx.common.oss.filesystem.FetchPolicy;
import com.alibaba.polardbx.common.oss.filesystem.FileSystemRateLimiter;
import com.alibaba.polardbx.common.oss.filesystem.GuavaFileSystemRateLimiter;
import com.alibaba.polardbx.common.oss.filesystem.OSSFileSystem;
import com.alibaba.polardbx.common.oss.filesystem.cache.CacheManager;
import com.alibaba.polardbx.common.oss.filesystem.cache.CacheType;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCacheManager;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCachingFileSystem;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.FileConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.time.parser.StringNumericParser;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static com.alibaba.polardbx.common.oss.filesystem.Constants.ACCESS_KEY_ID;
import static com.alibaba.polardbx.common.oss.filesystem.Constants.ACCESS_KEY_SECRET;
import static com.alibaba.polardbx.common.oss.filesystem.Constants.ENDPOINT_KEY;
import static com.alibaba.polardbx.common.oss.filesystem.Constants.OSS_FETCH_POLICY;

public class OSSInstanceInitializer {
    /**
     * oss://[accessKeyId:accessKeySecret@]bucket[.endpoint]/object/path
     */
    public static final String URI_FORMAT = "oss://%s/%s";

    public String accessKeyIdValue;
    public String accessKeySecretValue;
    public String bucketUri;
    public String endpointValue;
    public CachePolicy cachePolicy;

    public OSSInstanceInitializer() {
    }

    public static OSSInstanceInitializer newBuilder() {
        return new OSSInstanceInitializer();
    }

    public OSSInstanceInitializer accessKeyIdValue(String accessKeyIdValue) {
        this.accessKeyIdValue = accessKeyIdValue;
        return this;
    }

    public OSSInstanceInitializer accessKeySecretValue(String accessKeySecretValue) {
        this.accessKeySecretValue = accessKeySecretValue;
        return this;
    }

    public OSSInstanceInitializer bucketName(String bucketUri) {
        this.bucketUri = bucketUri;
        return this;
    }

    public OSSInstanceInitializer endpointValue(String endpointValue) {
        this.endpointValue = endpointValue;
        return this;
    }

    public OSSInstanceInitializer cachePolicy(CachePolicy cachePolicy) {
        this.cachePolicy = cachePolicy;
        return this;
    }

    public FileSystem initialize() {
        CacheManager cacheManager = null;
        FileSystem ossFileSystem = null;
        try {
            cacheManager = FileMergeCacheManager.createMergeCacheManager(Engine.OSS);
            URI ossFileUri = URI.create(this.bucketUri);
            ossFileSystem = createOSSFileSystem(ossFileUri,
                cachePolicy == CachePolicy.META_CACHE || cachePolicy == CachePolicy.META_AND_DATA_CACHE);
            URI fsUri = ossFileSystem.getUri();
            Configuration factoryConfig = new Configuration();
            final boolean validationEnabled = FileConfig.getInstance().getCacheConfig().isValidationEnabled();

            return new FileMergeCachingFileSystem(
                fsUri,
                factoryConfig,
                cacheManager,
                ossFileSystem,
                validationEnabled,
                true);
        } catch (Throwable t) {
            if (cacheManager != null) {
                try {
                    cacheManager.close();
                } catch (Throwable t1) {
                    // ignore
                }
            }
            if (ossFileSystem != null) {
                try {
                    ossFileSystem.close();
                } catch (Throwable t1) {
                    // ignore
                }
            }
            throw GeneralUtil.nestedException("Fail to create OSS file system!", t);
        }
    }

    private synchronized OSSFileSystem createOSSFileSystem(URI ossFileUri, boolean enableCache) throws
        IOException {
        FileSystemRateLimiter rateLimiter = FileSystemUtils.newRateLimiter();
        // oss file system
        // oss://[accessKeyId:accessKeySecret@]bucket[.endpoint]/object/path
        OSSFileSystem OSS_FILE_SYSTEM = new OSSFileSystem(enableCache, rateLimiter);
        Configuration fsConf = new Configuration();
        fsConf.set(ACCESS_KEY_ID, this.accessKeyIdValue);
        fsConf.set(ACCESS_KEY_SECRET, this.accessKeySecretValue);
        fsConf.set(ENDPOINT_KEY, this.endpointValue);
        fsConf.set(OSS_FETCH_POLICY, FetchPolicy.REQUESTED.name());
        OSS_FILE_SYSTEM.initialize(ossFileUri, fsConf);
        return OSS_FILE_SYSTEM;

    }

}
