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

/**
 *
 */
package com.alibaba.polardbx.ssl;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import java.util.Arrays;

/**
 * ssl constant values
 */
public class SslConstant {
    public static String PROTOCOL = "TLS";

    public static String[] enabledProtocols;

    static {
        SSLSocket sslSocket = null;
        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, null, null);
            sslSocket = (SSLSocket) sslContext.getSocketFactory().createSocket();
            enabledProtocols = sslSocket.getSupportedProtocols();
            System.out.println(String.format("The server supportedProtocols: %s", Arrays.toString(enabledProtocols)));
        } catch (Throwable t) {

        } finally {
            try {
                if (sslSocket != null) {
                    sslSocket.close();
                }
            } catch (Throwable t) {
                //ignore
            }
        }
    }

    public static final String KEY_STORE_PASSWORD = "9YG$Cd!Y$@3!^mGX";
    public static final String DEFAULT_ALGORITHM = "SunX509";
    public static final String KEY_STORE_TYPE = "JKS";

    @Deprecated
    public static final String SERVER_STORE_PASSWORD_KEY = "server.store.passwd";
    @Deprecated
    public static final String ALGORITHM_KEY = "ssl.KeyManagerFactory.algorithm";

    public static final String ENABLE_SSL = "sslEnable";

    // for new ssl logic
    public static final String SSL_KEY_PATH = "sslKeyPath";
    public static final String SSL_CERT_PATH = "sslCertPath";
    public static final String SSL_ROOT_CERT_PATH = "sslRootCertPath";
    public static final String CRT_BEGIN_DELIMITER = "-----BEGIN CERTIFICATE-----";
    public static final String CRT_END_DELIMITER = "-----END CERTIFICATE-----";
    public static final String KEY_BEGIN_DELIMITER = "-----BEGIN RSA PRIVATE KEY-----";
    public static final String KEY_END_DELIMITER = "-----END RSA PRIVATE KEY-----";
    public static final String CERT_ALIAS = "polardb-x-cert";
    public static final String KEY_ALIAS = "polardb-x-key";
    public static final String ROOT_CERT_ALIAS = "polardb-x-root-cert";
    public static final String PRIVATE_KEY_ALGORITHM = "RSA";
    public static final String CERTIFICATE_TYPE = "X.509";

    // for old ssl logic
    public static final String SERVER_KEY_STORE_KEY = "server.keystore";
    public static final String SERVER_TRUST_KEY_STORE_KEY = "server.trustkeystore";

    // polardb-x SSL variables
    public static final String SSL_VAR_HAVE_OPENSSL = "have_openssl";
    public static final String SSL_VAR_HAVE_SSL = "have_ssl";
    public static final String SSL_VAR_POLAR_X_SSL_CA = "polarx_ssl_ca";
    public static final String SSL_VAR_POLAR_X_SSL_CAPATH = "polarx_ssl_capath";
    public static final String SSL_VAR_POLAR_X_SSL_CERT = "polarx_ssl_cert";
    public static final String SSL_VAR_POLAR_X_SSL_CIPHER = "polarx_ssl_cipher";
    public static final String SSL_VAR_POLAR_X_SSL_CRL = "polarx_ssl_crl";
    public static final String SSL_VAR_POLAR_X_SSL_CRLPATH = "polarx_ssl_crlpath";
    public static final String SSL_VAR_POLAR_X_SSL_KEY = "polarx_ssl_key";
    public static final String SSL_VAR_SSL_CA = "ssl_ca";
    public static final String SSL_VAR_SSL_CAPATH = "ssl_capath";
    public static final String SSL_VAR_SSL_CERT = "ssl_cert";
    public static final String SSL_VAR_SSL_CIPHER = "ssl_cipher";
    public static final String SSL_VAR_SSL_CRL = "ssl_crl";
    public static final String SSL_VAR_SSL_CRLPATH = "ssl_crlpath";
    public static final String SSL_VAR_SSL_KEY = "ssl_key";
}
