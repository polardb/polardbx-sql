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
import com.alibaba.polardbx.gms.engine.decorator.impl.ABSFileSystemWrapper;
import com.alibaba.polardbx.gms.engine.decorator.impl.FileSystemStrategyImpl;
import com.alibaba.polardbx.gms.engine.decorator.impl.SecureABSFileSystemWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;

import static com.alibaba.polardbx.common.oss.filesystem.Constants.ABS_ACCOUNT_KEY_PATTERN;
import static com.alibaba.polardbx.common.oss.filesystem.Constants.AZURE_WASBS_SCHEME;

public class ABSInstanceInitializer {
    public String accessKeyIdValue;
    public String accessKeySecretValue;
    public String bucketUri;
    public String endpointValue;
    public CachePolicy cachePolicy;

    private ABSInstanceInitializer() {
    }

    public static ABSInstanceInitializer newBuilder() {
        return new ABSInstanceInitializer();
    }

    public ABSInstanceInitializer accessKeyIdValue(String accessKeyIdValue) {
        this.accessKeyIdValue = accessKeyIdValue;
        return this;
    }

    public ABSInstanceInitializer accessKeySecretValue(String accessKeySecretValue) {
        this.accessKeySecretValue = accessKeySecretValue;
        return this;
    }

    public ABSInstanceInitializer bucketName(String bucketUri) {
        this.bucketUri = bucketUri;
        return this;
    }

    public ABSInstanceInitializer endpointValue(String endpointValue) {
        this.endpointValue = endpointValue;
        return this;
    }

    public ABSInstanceInitializer cachePolicy(CachePolicy cachePolicy) {
        this.cachePolicy = cachePolicy;
        return this;
    }

    public FileSystem initialize() {
        CacheManager cacheManager = null;
        FileSystem absFileSystem = null;
        try {
            cacheManager = FileMergeCacheManager.createMergeCacheManager(Engine.S3);
            URI s3FileUri = URI.create(this.bucketUri);
            absFileSystem = createABSFileSystem(s3FileUri,
                cachePolicy == CachePolicy.META_CACHE || cachePolicy == CachePolicy.META_AND_DATA_CACHE);
            URI fsUri = absFileSystem.getUri();
            Configuration factoryConfig = new Configuration();
            final boolean validationEnabled = FileConfig.getInstance().getCacheConfig().isValidationEnabled();

            return new FileMergeCachingFileSystem(fsUri, factoryConfig, cacheManager, absFileSystem, validationEnabled,
                true);
        } catch (Throwable t) {
            if (cacheManager != null) {
                try {
                    cacheManager.close();
                } catch (Throwable t1) {
                    // ignore
                }
            }
            if (absFileSystem != null) {
                try {
                    absFileSystem.close();
                } catch (Throwable t1) {
                    // ignore
                }
            }
            throw GeneralUtil.nestedException("Fail to create ABS file system!", t);
        }
    }

    private synchronized FileSystem createABSFileSystem(URI absFileUri, boolean enableCache) throws IOException {
        FileSystemRateLimiter rateLimiter = FileSystemUtils.newRateLimiter();
        FileSystemStrategy strategy = new FileSystemStrategyImpl(enableCache, rateLimiter);
        FileSystemDecorator fsDecorator;
        if (AZURE_WASBS_SCHEME.equalsIgnoreCase(absFileUri.getScheme())) {
            // wasbs://
            fsDecorator = new FileSystemDecorator(new SecureABSFileSystemWrapper(), strategy);
        } else {
            // wasb://
            fsDecorator = new FileSystemDecorator(new ABSFileSystemWrapper(), strategy);
        }
        Configuration fsConf = new Configuration();
        String absAccountKey = String.format(ABS_ACCOUNT_KEY_PATTERN, this.accessKeyIdValue, this.endpointValue);
        fsConf.set(absAccountKey, this.accessKeySecretValue);
        fsDecorator.initialize(absFileUri, fsConf);
        return fsDecorator;
    }
}
