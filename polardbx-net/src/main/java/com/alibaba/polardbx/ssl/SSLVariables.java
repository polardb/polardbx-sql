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

package com.alibaba.polardbx.ssl;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;

import static com.alibaba.polardbx.ssl.SslConstant.SSL_VAR_HAVE_OPENSSL;
import static com.alibaba.polardbx.ssl.SslConstant.SSL_VAR_HAVE_SSL;
import static com.alibaba.polardbx.ssl.SslConstant.SSL_VAR_POLAR_X_SSL_CA;
import static com.alibaba.polardbx.ssl.SslConstant.SSL_VAR_POLAR_X_SSL_CAPATH;
import static com.alibaba.polardbx.ssl.SslConstant.SSL_VAR_POLAR_X_SSL_CERT;
import static com.alibaba.polardbx.ssl.SslConstant.SSL_VAR_POLAR_X_SSL_CIPHER;
import static com.alibaba.polardbx.ssl.SslConstant.SSL_VAR_POLAR_X_SSL_CRL;
import static com.alibaba.polardbx.ssl.SslConstant.SSL_VAR_POLAR_X_SSL_CRLPATH;
import static com.alibaba.polardbx.ssl.SslConstant.SSL_VAR_POLAR_X_SSL_KEY;
import static com.alibaba.polardbx.ssl.SslConstant.SSL_VAR_SSL_CA;
import static com.alibaba.polardbx.ssl.SslConstant.SSL_VAR_SSL_CAPATH;
import static com.alibaba.polardbx.ssl.SslConstant.SSL_VAR_SSL_CERT;
import static com.alibaba.polardbx.ssl.SslConstant.SSL_VAR_SSL_CIPHER;
import static com.alibaba.polardbx.ssl.SslConstant.SSL_VAR_SSL_CRL;
import static com.alibaba.polardbx.ssl.SslConstant.SSL_VAR_SSL_CRLPATH;
import static com.alibaba.polardbx.ssl.SslConstant.SSL_VAR_SSL_KEY;
import static com.alibaba.polardbx.ssl.SslContextFactory.checkFile;

public class SSLVariables {
    /**
     * For have_ssl:
     * If supports SSL connections, DISABLED if the server was compiled with SSL support.
     * For have_openssl:
     * This variable is a synonym for have_ssl.
     */
    public static final Supplier<Object> SSL_VAR_HAVE_SSL_SUPPLIER = () -> {
        String sslEnable = System.getProperty(SslConstant.ENABLE_SSL);
        if (!StringUtils.isEmpty(sslEnable) && SslContextFactory.isSuccess()) {
            boolean enableSSL = Boolean.valueOf(sslEnable);
            String haveSSL = enableSSL ? "ENABLE" : "DISABLED";
            return haveSSL;
        } else {
            return "DISABLED";
        }
    };

    /**
     * For ssl_ca:
     * The path name of the Certificate Authority (CA) certificate file in PEM format.
     * The file contains a list of trusted SSL Certificate Authorities.
     */
    public static final Supplier<Object> SSL_VAR_SSL_CA_SUPPLIER = () -> {
        String sslRootCertPath = System.getProperty(SslConstant.SSL_ROOT_CERT_PATH);
        if (checkFile(sslRootCertPath)) {
            return sslRootCertPath;
        } else {
            return "";
        }
    };

    /**
     * For ssl_capath:
     * The path name of the directory that contains trusted SSL Certificate Authority (CA) certificate files in PEM format.
     * Support for this capability depends on the SSL library used to compile MySQL
     */
    public static final Supplier<Object> SSL_VAR_SSL_CAPATH_SUPPLIER = () -> {
        String sslRootCertPath = System.getProperty(SslConstant.SSL_ROOT_CERT_PATH);
        File file = null;
        boolean exists = !StringUtils.isEmpty(sslRootCertPath)
            && (file = new File(sslRootCertPath)).exists()
            && !file.isDirectory();
        if (exists && file != null) {
            return file.getParent();
        } else {
            return "";
        }
    };

    /**
     * For ssl_cert:
     * The path name of the server SSL public key certificate file in PEM format.
     */
    public static final Supplier<Object> SSL_VAR_SSL_CERT_SUPPLIER = () -> {
        String sslCertPath = System.getProperty(SslConstant.SSL_CERT_PATH);
        if (checkFile(sslCertPath)) {
            return sslCertPath;
        } else {
            return "";
        }
    };

    /**
     * For ssl_cipher:
     * The list of permissible ciphers for connection encryption.
     * If no cipher in the list is supported, encrypted connections do not work.
     * For ssl_crl:
     * The path name of the file containing certificate revocation lists in PEM format.
     * Support for revocation-list capability depends on the SSL library used to compile MySQL.
     * For ssl_crlpath:
     * The path of the directory that contains certificate revocation-list files in PEM format.
     * Support for revocation-list capability depends on the SSL library used to compile MySQL.
     */
    public static final Supplier<Object> SSL_VAR_SSL_EMPTY_SUPPLIER = () -> {
        // Don't show to users.
        return "";
    };

    /**
     * For ssl_key:
     * The path name of the server SSL private key file in PEM format. For better security,
     * use a certificate with an RSA key size of at least 2048 bits.
     */
    public static final Supplier<Object> SSL_VAR_SSL_KEY_SUPPLIER = () -> {
        String sslKeyPath = System.getProperty(SslConstant.SSL_KEY_PATH);
        if (checkFile(sslKeyPath)) {
            return sslKeyPath;
        } else {
            return "";
        }
    };

    public static final Map<String, Supplier<Object>> SSL_VAR_SUPPLIERS;

    static {
        ImmutableMap.Builder<String, Supplier<Object>> builder = ImmutableMap.builder();

        builder.put(SSL_VAR_HAVE_SSL, SSL_VAR_HAVE_SSL_SUPPLIER);
        builder.put(SSL_VAR_HAVE_OPENSSL, SSL_VAR_HAVE_SSL_SUPPLIER);
        builder.put(SSL_VAR_POLAR_X_SSL_CA, SSL_VAR_SSL_CA_SUPPLIER);
        builder.put(SSL_VAR_SSL_CA, SSL_VAR_SSL_CA_SUPPLIER);
        builder.put(SSL_VAR_POLAR_X_SSL_CAPATH, SSL_VAR_SSL_CAPATH_SUPPLIER);
        builder.put(SSL_VAR_SSL_CAPATH, SSL_VAR_SSL_CAPATH_SUPPLIER);
        builder.put(SSL_VAR_POLAR_X_SSL_CERT, SSL_VAR_SSL_CERT_SUPPLIER);
        builder.put(SSL_VAR_SSL_CERT, SSL_VAR_SSL_CERT_SUPPLIER);

        builder.put(SSL_VAR_POLAR_X_SSL_CIPHER, SSL_VAR_SSL_EMPTY_SUPPLIER);
        builder.put(SSL_VAR_SSL_CIPHER, SSL_VAR_SSL_EMPTY_SUPPLIER);
        builder.put(SSL_VAR_POLAR_X_SSL_CRL, SSL_VAR_SSL_EMPTY_SUPPLIER);
        builder.put(SSL_VAR_SSL_CRL, SSL_VAR_SSL_EMPTY_SUPPLIER);
        builder.put(SSL_VAR_POLAR_X_SSL_CRLPATH, SSL_VAR_SSL_EMPTY_SUPPLIER);
        builder.put(SSL_VAR_SSL_CRLPATH, SSL_VAR_SSL_EMPTY_SUPPLIER);

        builder.put(SSL_VAR_POLAR_X_SSL_KEY, SSL_VAR_SSL_KEY_SUPPLIER);
        builder.put(SSL_VAR_SSL_KEY, SSL_VAR_SSL_KEY_SUPPLIER);

        SSL_VAR_SUPPLIERS = builder.build();
    }

    public static void fill(final TreeMap<String, Object> variables) {
        SSL_VAR_SUPPLIERS.entrySet().stream()
            .filter(e ->
                variables.containsKey(e.getKey())
            )
            .forEach(e ->
                variables.put(e.getKey(), e.getValue().get())
            );
    }
}
