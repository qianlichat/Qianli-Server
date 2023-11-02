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

public class RepeatedUseKEMSignedPreKeyStore extends RepeatedUseSignedPreKeyStore<KEMSignedPreKey> {

  public RepeatedUseKEMSignedPreKeyStore(final DynamoDbAsyncClient dynamoDbAsyncClient, final String tableName) {
    super(dynamoDbAsyncClient, tableName);
  }

  @Override
  protected Map<String, AttributeValue> getItemFromPreKey(final UUID accountUuid, final long deviceId, final KEMSignedPreKey signedPreKey) {

    return Map.of(
        KEY_ACCOUNT_UUID, getPartitionKey(accountUuid),
        KEY_DEVICE_ID, getSortKey(deviceId),
        ATTR_KEY_ID, AttributeValues.fromLong(signedPreKey.keyId()),
        ATTR_PUBLIC_KEY, AttributeValues.fromByteArray(signedPreKey.serializedPublicKey()),
        ATTR_SIGNATURE, AttributeValues.fromByteArray(signedPreKey.signature()));
  }

  @Override
  protected KEMSignedPreKey getPreKeyFromItem(final Map<String, AttributeValue> item) {
    try {
      return new KEMSignedPreKey(
          Long.parseLong(item.get(ATTR_KEY_ID).n()),
          new KEMPublicKey(item.get(ATTR_PUBLIC_KEY).b().asByteArray()),
          item.get(ATTR_SIGNATURE).b().asByteArray());
    } catch (final InvalidKeyException e) {
      // This should never happen since we're serializing keys directly from `KEMPublicKey` instances on the way in
      throw new IllegalArgumentException(e);
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(RepeatedUseKEMSignedPreKeyStore.class);

  @Override
  public CompletableFuture<Void> delete(final UUID identifier, final long deviceId) {
//    logger.info("delete 4 for :" + identifier +",deviceId = " + deviceId);
    final CompletableFuture<Void> delete = super.delete(identifier, deviceId);
    delete.exceptionally(ex -> {
//      logger.error("delete 4 for", ex);
      throw ExceptionUtils.wrap(ex);
    });
    return delete;
  }

  @Override
  public CompletableFuture<Void> delete(final UUID identifier) {
//    logger.info("delete 4 for :" + identifier);
    return super.delete(identifier);
  }
}
