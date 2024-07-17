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
import com.alibaba.polardbx.common.oss.filesystem.FileSystemRateLimiter;
import com.alibaba.polardbx.common.oss.filesystem.cache.CacheManager;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCacheManager;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCachingFileSystem;
import com.alibaba.polardbx.common.properties.FileConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.engine.decorator.FileSystemDecorator;
import com.alibaba.polardbx.gms.engine.decorator.FileSystemStrategy;
import com.alibaba.polardbx.gms.engine.decorator.impl.FileSystemStrategyImpl;
import com.alibaba.polardbx.gms.engine.decorator.impl.S3AFileSystemWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;

import static com.alibaba.polardbx.common.oss.filesystem.Constants.S3_ACCESS_KEY;
import static com.alibaba.polardbx.common.oss.filesystem.Constants.S3_SECRET_KEY;

public class S3InstanceInitializer {

    public String accessKeyIdValue;
    public String accessKeySecretValue;
    public String bucketUri;
    public CachePolicy cachePolicy;

    private S3InstanceInitializer() {
    }

    public static S3InstanceInitializer newBuilder() {
        return new S3InstanceInitializer();
    }

    public S3InstanceInitializer accessKeyIdValue(String accessKeyIdValue) {
        this.accessKeyIdValue = accessKeyIdValue;
        return this;
    }

    public S3InstanceInitializer accessKeySecretValue(String accessKeySecretValue) {
        this.accessKeySecretValue = accessKeySecretValue;
        return this;
    }

    public S3InstanceInitializer bucketName(String bucketUri) {
        this.bucketUri = bucketUri;
        return this;
    }

    public S3InstanceInitializer cachePolicy(CachePolicy cachePolicy) {
        this.cachePolicy = cachePolicy;
        return this;
    }

    public FileSystem initialize() {
        CacheManager cacheManager = null;
        FileSystem s3FileSystem = null;
        try {
            cacheManager = FileMergeCacheManager.createMergeCacheManager(Engine.S3);
            URI s3FileUri = URI.create(this.bucketUri);
            s3FileSystem = createS3FileSystem(s3FileUri,
                cachePolicy == CachePolicy.META_CACHE || cachePolicy == CachePolicy.META_AND_DATA_CACHE);
            URI fsUri = s3FileSystem.getUri();
            Configuration factoryConfig = new Configuration();
            final boolean validationEnabled = FileConfig.getInstance().getCacheConfig().isValidationEnabled();

            return new FileMergeCachingFileSystem(fsUri, factoryConfig, cacheManager, s3FileSystem, validationEnabled,
                true);
        } catch (Throwable t) {
            if (cacheManager != null) {
                try {
                    cacheManager.close();
                } catch (Throwable t1) {
                    // ignore
                }
            }
            if (s3FileSystem != null) {
                try {
                    s3FileSystem.close();
                } catch (Throwable t1) {
                    // ignore
                }
            }
            throw GeneralUtil.nestedException("Fail to create S3 file system!", t);
        }
    }

    private synchronized FileSystem createS3FileSystem(URI s3FileUri, boolean enableCache) throws IOException {
        FileSystemRateLimiter rateLimiter = FileSystemUtils.newRateLimiter();
        FileSystemStrategy strategy = new FileSystemStrategyImpl(enableCache, rateLimiter);
        S3AFileSystemWrapper fs = new S3AFileSystemWrapper();
        FileSystemDecorator fsDecorator = new FileSystemDecorator(fs, strategy);
        Configuration fsConf = new Configuration();
        fsConf.set(S3_ACCESS_KEY, this.accessKeyIdValue);
        fsConf.set(S3_SECRET_KEY, this.accessKeySecretValue);
        fsDecorator.initialize(s3FileUri, fsConf);
        return fsDecorator;
    }
}
