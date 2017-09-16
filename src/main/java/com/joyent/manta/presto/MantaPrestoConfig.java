/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.joyent.manta.config.ConfigContext;
import io.airlift.configuration.Config;
import com.joyent.manta.config.MapConfigContext;
import com.joyent.manta.config.BaseChainedConfigContext;
import com.joyent.manta.config.EncryptionAuthenticationMode;

/**
 * Manta configuration provider for the Presto Connector.
 */
public class MantaPrestoConfig extends BaseChainedConfigContext {

    public MantaPrestoConfig() {
    }

    public MantaPrestoConfig(final ConfigContext defaultingContext) {
        super(defaultingContext);
    }

    @Config(MapConfigContext.MANTA_URL_KEY)
    @Override
    public BaseChainedConfigContext setMantaURL(final String mantaURL) {
        return super.setMantaURL(mantaURL);
    }

    @Config(MapConfigContext.MANTA_USER_KEY)
    @Override
    public BaseChainedConfigContext setMantaUser(final String mantaUser) {
        return super.setMantaUser(mantaUser);
    }

    @Config(MapConfigContext.MANTA_KEY_ID_KEY)
    @Override
    public BaseChainedConfigContext setMantaKeyId(final String mantaKeyId) {
        return super.setMantaKeyId(mantaKeyId);
    }

    @Config(MapConfigContext.MANTA_KEY_PATH_KEY)
    @Override
    public BaseChainedConfigContext setMantaKeyPath(final String mantaKeyPath) {
        return super.setMantaKeyPath(mantaKeyPath);
    }

    @Config(MapConfigContext.MANTA_TIMEOUT_KEY)
    @Override
    public BaseChainedConfigContext setTimeout(final Integer timeout) {
        return super.setTimeout(timeout);
    }

    @Config(MapConfigContext.MANTA_RETRIES_KEY)
    @Override
    public BaseChainedConfigContext setRetries(final Integer retries) {
        return super.setRetries(retries);
    }

    @Config(MapConfigContext.MANTA_MAX_CONNS_KEY)
    @Override
    public BaseChainedConfigContext setMaximumConnections(final Integer maxConns) {
        return super.setMaximumConnections(maxConns);
    }

    @Config(MapConfigContext.MANTA_PRIVATE_KEY_CONTENT_KEY)
    @Override
    public BaseChainedConfigContext setPrivateKeyContent(final String privateKeyContent) {
        return super.setPrivateKeyContent(privateKeyContent);
    }

    @Config(MapConfigContext.MANTA_PASSWORD_KEY)
    @Override
    public BaseChainedConfigContext setPassword(final String password) {
        return super.setPassword(password);
    }

    @Config(MapConfigContext.MANTA_HTTP_BUFFER_SIZE_KEY)
    @Override
    public BaseChainedConfigContext setHttpBufferSize(final Integer httpBufferSize) {
        return super.setHttpBufferSize(httpBufferSize);
    }

    @Config(MapConfigContext.MANTA_HTTPS_PROTOCOLS_KEY)
    @Override
    public BaseChainedConfigContext setHttpsProtocols(final String httpsProtocols) {
        return super.setHttpsProtocols(httpsProtocols);
    }

    @Config(MapConfigContext.MANTA_HTTPS_CIPHERS_KEY)
    @Override
    public BaseChainedConfigContext setHttpsCipherSuites(final String httpsCipherSuites) {
        return super.setHttpsCipherSuites(httpsCipherSuites);
    }

    @Config(MapConfigContext.MANTA_NO_AUTH_KEY)
    @Override
    public BaseChainedConfigContext setNoAuth(final Boolean noAuth) {
        return super.setNoAuth(noAuth);
    }

    @Config(MapConfigContext.MANTA_NO_NATIVE_SIGS_KEY)
    @Override
    public BaseChainedConfigContext setDisableNativeSignatures(final Boolean disableNativeSignatures) {
        return super.setDisableNativeSignatures(disableNativeSignatures);
    }

    @Config(MapConfigContext.MANTA_TCP_SOCKET_TIMEOUT_KEY)
    @Override
    public BaseChainedConfigContext setTcpSocketTimeout(final Integer tcpSocketTimeout) {
        return super.setTcpSocketTimeout(tcpSocketTimeout);
    }

    @Config(MapConfigContext.MANTA_VERIFY_UPLOADS_KEY)
    @Override
    public BaseChainedConfigContext setVerifyUploads(final Boolean verify) {
        return super.setVerifyUploads(verify);
    }

    @Config(MapConfigContext.MANTA_UPLOAD_BUFFER_SIZE_KEY)
    @Override
    public BaseChainedConfigContext setUploadBufferSize(final Integer size) {
        return super.setUploadBufferSize(size);
    }

    @Config(MapConfigContext.MANTA_CLIENT_ENCRYPTION_ENABLED_KEY)
    @Override
    public BaseChainedConfigContext setClientEncryptionEnabled(final Boolean clientEncryptionEnabled) {
        return super.setClientEncryptionEnabled(clientEncryptionEnabled);
    }

    @Config(MapConfigContext.MANTA_ENCRYPTION_KEY_ID_KEY)
    @Override
    public BaseChainedConfigContext setEncryptionKeyId(final String keyId) {
        return super.setEncryptionKeyId(keyId);
    }

    @Config(MapConfigContext.MANTA_ENCRYPTION_ALGORITHM_KEY)
    @Override
    public BaseChainedConfigContext setEncryptionAlgorithm(final String algorithm) {
        return super.setEncryptionAlgorithm(algorithm);
    }

    @Config(MapConfigContext.MANTA_PERMIT_UNENCRYPTED_DOWNLOADS_KEY)
    @Override
    public BaseChainedConfigContext setPermitUnencryptedDownloads(final Boolean permitUnencryptedDownloads) {
        return super.setPermitUnencryptedDownloads(permitUnencryptedDownloads);
    }

    @Config(MapConfigContext.MANTA_ENCRYPTION_AUTHENTICATION_MODE_KEY)
    @Override
    public BaseChainedConfigContext setEncryptionAuthenticationMode(
            final EncryptionAuthenticationMode encryptionAuthenticationMode) {
        return super.setEncryptionAuthenticationMode(encryptionAuthenticationMode);
    }

    @Config(MapConfigContext.MANTA_ENCRYPTION_PRIVATE_KEY_PATH_KEY)
    @Override
    public BaseChainedConfigContext setEncryptionPrivateKeyPath(final String encryptionPrivateKeyPath) {
        return super.setEncryptionPrivateKeyPath(encryptionPrivateKeyPath);
    }

    @Config(MapConfigContext.MANTA_ENCRYPTION_PRIVATE_KEY_BYTES_KEY)
    @Override
    public BaseChainedConfigContext setEncryptionPrivateKeyBytes(final byte[] encryptionPrivateKeyBytes) {
        return super.setEncryptionPrivateKeyBytes(encryptionPrivateKeyBytes);
    }
}
