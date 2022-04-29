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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;

/**
 * SSL context initialization.
 */
public final class SslContextFactory {

    private static SSLContext SERVER_CONTEXT;

    private static volatile boolean INIT = false;

    private static volatile boolean isSuccess = false;

    private static final Logger LOGGER = LoggerFactory.getLogger(SslContextFactory.class);

    public static SSLContext getServerContext() throws Throwable {
        if (INIT) {
            return SERVER_CONTEXT;
        }

        synchronized (SslContextFactory.class) {
            if (INIT) {
                return SERVER_CONTEXT;
            }

            Provider sunjceProvider = Security.getProvider("SUNJCE");
            if (sunjceProvider != null) {
                Security.addProvider(sunjceProvider);
            }

            SSLContext serverContext = null;

            // read new & old ssl configurations.
            String sslKeyPath = System.getProperty(SslConstant.SSL_KEY_PATH);
            String sslCertPath = System.getProperty(SslConstant.SSL_CERT_PATH);
            String sslRootCertPath = System.getProperty(SslConstant.SSL_ROOT_CERT_PATH);
            String keyStorePath = System.getProperty(SslConstant.SERVER_KEY_STORE_KEY);
            String trustStorePath = System.getProperty(SslConstant.SERVER_TRUST_KEY_STORE_KEY);

            if (checkFile(sslKeyPath)
                && checkFile(sslCertPath)
                && checkFile(sslRootCertPath)) {
                // new logic of reading .key and .cert file.
                serverContext = sslContextInit(sslCertPath, sslKeyPath, sslRootCertPath);
                LOGGER.warn(
                    String.format("SSL config success, the configurations are %s, the getProtocol is %s",
                        newConfigLogMessage(sslKeyPath, sslCertPath, sslRootCertPath), serverContext.getProtocol()));
            } else if (checkFile(keyStorePath)
                && checkFile(trustStorePath)) {
                // old logic of reading keystore & truststore from path.
                serverContext = oldSSLContextInit(keyStorePath, trustStorePath);
                LOGGER.warn(
                    String.format("SSL config success, the configurations are %s, the getProtocol is %s",
                        oldConfigLogMessage(keyStorePath, trustStorePath), serverContext.getProtocol())
                );
            }
            if (serverContext == null) {
                String errorMessage;
                LOGGER.error(
                    errorMessage = "Failed to initialize the server-side SSLContext, the configurations are: "
                        + newConfigLogMessage(sslKeyPath, sslCertPath, sslRootCertPath) + " and "
                        + oldConfigLogMessage(keyStorePath, trustStorePath));
                GeneralUtil.nestedException(errorMessage);
            } else {
                isSuccess = true;
            }

            SERVER_CONTEXT = serverContext;
            INIT = true;

            return SERVER_CONTEXT;
        }
    }

    private static String oldConfigLogMessage(String keyStorePath, String trustStorePath) {
        return "keyStorePath=" + keyStorePath + ", trustStorePath=" + trustStorePath;
    }

    private static String newConfigLogMessage(String sslKeyPath, String sslCertPath, String sslRootCertPath) {
        return "sslKeyPath=" + sslKeyPath + ", sslCertPath=" + sslCertPath + ", rootCertPath=" + sslRootCertPath;
    }

    static boolean checkFile(String path) {
        File file;
        return !StringUtils.isEmpty(path)
            && (file = new File(path)).exists()
            && !file.isDirectory();
    }

    private static SSLContext oldSSLContextInit(String keyStorePath, String trustStorePath) {
        try {
            char[] password = SslConstant.KEY_STORE_PASSWORD.toCharArray();

            // load keystore
            KeyStore ks = KeyStore.getInstance(SslConstant.KEY_STORE_TYPE);
            ks.load(new FileInputStream(keyStorePath), password);
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(SslConstant.DEFAULT_ALGORITHM);
            kmf.init(ks, password);

            // load truststore
            KeyStore tks = KeyStore.getInstance(SslConstant.KEY_STORE_TYPE);
            tks.load(new FileInputStream(trustStorePath), password);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(SslConstant.DEFAULT_ALGORITHM);
            tmf.init(tks);

            // init ssl context from key store and trust store.
            SSLContext serverContext = SSLContext.getInstance(SslConstant.PROTOCOL);
            serverContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
            return serverContext;
        } catch (Exception e) {
            LOGGER.error("SSL config failed, the configurations are " +
                oldConfigLogMessage(keyStorePath, trustStorePath), e);
            GeneralUtil.nestedException("Failed to initialize the server-side SSLContext", e);
        }
        return null;
    }

    protected static SSLContext sslContextInit(String crtPath, String keyPath, String rootCrtPath) {
        try {
            // add more security utilities.
            Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());

            // parse file content according to delimiters.
            byte[] certBytes = parseFileContent(
                FileUtils.readFileToByteArray(new File(crtPath)),
                SslConstant.CRT_BEGIN_DELIMITER,
                SslConstant.CRT_END_DELIMITER);
            byte[] keyBytes = parseFileContent(
                FileUtils.readFileToByteArray(new File(keyPath)),
                SslConstant.KEY_BEGIN_DELIMITER,
                SslConstant.KEY_END_DELIMITER);
            byte[] rootCertBytes = parseFileContent(
                FileUtils.readFileToByteArray(new File(rootCrtPath)),
                SslConstant.CRT_BEGIN_DELIMITER,
                SslConstant.CRT_END_DELIMITER);

            // create certificate & private key from contents.
            X509Certificate cert = generateCertificateFromDER(certBytes);
            RSAPrivateKey key = generatePrivateKeyFromDER(keyBytes);
            X509Certificate rootCert = generateCertificateFromDER(rootCertBytes);

            char[] password = SslConstant.KEY_STORE_PASSWORD.toCharArray();

            // init keystore from certificate & private key.
            KeyStore keystore = KeyStore.getInstance(SslConstant.KEY_STORE_TYPE);
            keystore.load(null);
            keystore.setCertificateEntry(SslConstant.CERT_ALIAS, cert);
            keystore.setKeyEntry(SslConstant.KEY_ALIAS, key, password, new Certificate[] {cert});
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(SslConstant.DEFAULT_ALGORITHM);
            kmf.init(keystore, password);

            // init truststore from certificate
            KeyStore trustStore = KeyStore.getInstance(SslConstant.KEY_STORE_TYPE);
            trustStore.load(null);
            trustStore.setCertificateEntry(SslConstant.ROOT_CERT_ALIAS, rootCert);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(SslConstant.DEFAULT_ALGORITHM);
            tmf.init(trustStore);

            // init ssl context from key store and trust store.
            SSLContext context = SSLContext.getInstance(SslConstant.PROTOCOL);
            context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
            return context;
        } catch (Exception e) {
            LOGGER.error("SSL config failed, the configurations are " +
                newConfigLogMessage(keyPath, crtPath, rootCrtPath), e);
            GeneralUtil.nestedException("Failed to initialize the server-side SSLContext", e);
        }
        return null;
    }

    protected static byte[] parseFileContent(byte[] pem, String beginDelimiter, String endDelimiter) {
        String data = new String(pem);
        String[] tokens = data.split(beginDelimiter);
        tokens = tokens[1].split(endDelimiter);
        return Base64.getDecoder().decode(tokens[0].replaceAll("[\r\n]", ""));
    }

    protected static RSAPrivateKey generatePrivateKeyFromDER(byte[] keyBytes) throws InvalidKeySpecException,
        NoSuchAlgorithmException {
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
        KeyFactory factory = KeyFactory.getInstance(SslConstant.PRIVATE_KEY_ALGORITHM);
        return (RSAPrivateKey) factory.generatePrivate(spec);
    }

    protected static X509Certificate generateCertificateFromDER(byte[] certBytes) throws CertificateException {
        CertificateFactory factory = CertificateFactory.getInstance(SslConstant.CERTIFICATE_TYPE);
        return (X509Certificate) factory.generateCertificate(new ByteArrayInputStream(certBytes));
    }

    public static void reset() {
        INIT = false;
    }

    private SslContextFactory() {
        // Unused
    }

    public static boolean isSuccess() {
        return isSuccess;
    }

    public static boolean startSupportSsl() {
        String sslKeyPath = System.getProperty(SslConstant.SSL_KEY_PATH);
        String sslCertPath = System.getProperty(SslConstant.SSL_CERT_PATH);
        String sslRootCertPath = System.getProperty(SslConstant.SSL_ROOT_CERT_PATH);
        String keyStorePath = System.getProperty(SslConstant.SERVER_KEY_STORE_KEY);
        String trustStorePath = System.getProperty(SslConstant.SERVER_TRUST_KEY_STORE_KEY);

        boolean supportNewConfig = checkFile(sslKeyPath)
            && checkFile(sslCertPath)
            && checkFile(sslRootCertPath);

        boolean supportOldConfig = checkFile(keyStorePath)
            && checkFile(trustStorePath);

        return supportNewConfig || supportOldConfig;
    }
}
