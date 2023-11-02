/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.kem.KEMPublicKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class SingleUseKEMPreKeyStore extends SingleUsePreKeyStore<KEMSignedPreKey> {

  protected SingleUseKEMPreKeyStore(final DynamoDbAsyncClient dynamoDbAsyncClient, final String tableName) {
    super(dynamoDbAsyncClient, tableName);
  }

  @Override
  protected Map<String, AttributeValue> getItemFromPreKey(final UUID identifier, final long deviceId, final KEMSignedPreKey signedPreKey) {
    return Map.of(
        KEY_ACCOUNT_UUID, getPartitionKey(identifier),
        KEY_DEVICE_ID_KEY_ID, getSortKey(deviceId, signedPreKey.keyId()),
        ATTR_PUBLIC_KEY, AttributeValues.fromByteArray(signedPreKey.serializedPublicKey()),
        ATTR_SIGNATURE, AttributeValues.fromByteArray(signedPreKey.signature()));
  }

  @Override
  protected KEMSignedPreKey getPreKeyFromItem(final Map<String, AttributeValue> item) {
    final long keyId = item.get(KEY_DEVICE_ID_KEY_ID).b().asByteBuffer().getLong(8);
    final byte[] publicKey = item.get(ATTR_PUBLIC_KEY).b().asByteArray();
    final byte[] signature = item.get(ATTR_SIGNATURE).b().asByteArray();

    try {
      return new KEMSignedPreKey(keyId, new KEMPublicKey(publicKey), signature);
    } catch (final InvalidKeyException e) {
      // This should never happen since we're serializing keys directly from `KEMPublicKey` instances on the way in
      throw new IllegalArgumentException(e);
    }
  }

//  private static final Logger logger = LoggerFactory.getLogger(SingleUseKEMPreKeyStore.class);

  @Override
  public CompletableFuture<Void> delete(final UUID identifier, final long deviceId) {
//    logger.info("delete 2 for :" + identifier +",deviceId = " + deviceId);
    final CompletableFuture<Void> delete = super.delete(identifier, deviceId);
    delete.exceptionally(ex -> {
//      logger.error("delete 2 for", ex);
      throw ExceptionUtils.wrap(ex);
    });
    return delete;
  }

  @Override
  public CompletableFuture<Void> delete(final UUID identifier) {
//    logger.info("delete 2 for :" + identifier);
    return super.delete(identifier);
  }
}
