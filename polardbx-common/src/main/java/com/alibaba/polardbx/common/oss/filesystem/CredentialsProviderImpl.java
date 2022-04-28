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

package com.alibaba.polardbx.common.oss.filesystem;

import com.aliyun.oss.common.auth.Credentials;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.DefaultCredentials;
import com.aliyun.oss.common.auth.InvalidCredentialsException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

import static com.alibaba.polardbx.common.oss.filesystem.Constants.ACCESS_KEY_ID;
import static com.alibaba.polardbx.common.oss.filesystem.Constants.ACCESS_KEY_SECRET;
import static com.alibaba.polardbx.common.oss.filesystem.Constants.SECURITY_TOKEN;

/**
 * Support session credentials for authenticating.
 */
public class CredentialsProviderImpl implements CredentialsProvider {
    private Credentials credentials = null;

    public CredentialsProviderImpl(Configuration conf) throws IOException {
        String accessKeyId;
        String accessKeySecret;
        String securityToken;
        try {
            accessKeyId = OSSUtils.getValueWithKey(conf, ACCESS_KEY_ID);
            accessKeySecret = OSSUtils.getValueWithKey(conf, ACCESS_KEY_SECRET);
        } catch (IOException e) {
            throw new InvalidCredentialsException(e);
        }

        try {
            securityToken = OSSUtils.getValueWithKey(conf, SECURITY_TOKEN);
        } catch (IOException e) {
            securityToken = null;
        }

        if (StringUtils.isEmpty(accessKeyId)
            || StringUtils.isEmpty(accessKeySecret)) {
            throw new InvalidCredentialsException(
                "AccessKeyId and AccessKeySecret should not be null or empty.");
        }

        if (StringUtils.isNotEmpty(securityToken)) {
            credentials = new DefaultCredentials(accessKeyId, accessKeySecret,
                securityToken);
        } else {
            credentials = new DefaultCredentials(accessKeyId, accessKeySecret);
        }
    }

    @Override
    public void setCredentials(Credentials creds) {
        if (creds == null) {
            throw new InvalidCredentialsException("Credentials should not be null.");
        }

        credentials = creds;
    }

    @Override
    public Credentials getCredentials() {
        if (credentials == null) {
            throw new InvalidCredentialsException("Invalid credentials");
        }

        return credentials;
    }
}

