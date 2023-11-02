/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.ECPreKey;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

public class SingleUseECPreKeyStore extends SingleUsePreKeyStore<ECPreKey> {
  private static final String PARSE_BYTE_ARRAY_COUNTER_NAME = name(SingleUseECPreKeyStore.class, "parseByteArray");

  protected SingleUseECPreKeyStore(final DynamoDbAsyncClient dynamoDbAsyncClient, final String tableName) {
    super(dynamoDbAsyncClient, tableName);
  }

  @Override
  protected Map<String, AttributeValue> getItemFromPreKey(final UUID identifier, final long deviceId, final ECPreKey preKey) {
    return Map.of(
        KEY_ACCOUNT_UUID, getPartitionKey(identifier),
        KEY_DEVICE_ID_KEY_ID, getSortKey(deviceId, preKey.keyId()),
        ATTR_PUBLIC_KEY, AttributeValues.fromByteArray(preKey.serializedPublicKey()));
  }

  @Override
  protected ECPreKey getPreKeyFromItem(final Map<String, AttributeValue> item) {
    final long keyId = item.get(KEY_DEVICE_ID_KEY_ID).b().asByteBuffer().getLong(8);
    final byte[] publicKey = AttributeValues.extractByteArray(item.get(ATTR_PUBLIC_KEY), PARSE_BYTE_ARRAY_COUNTER_NAME);

    try {
      return new ECPreKey(keyId, new ECPublicKey(publicKey));
    } catch (final InvalidKeyException e) {
      // This should never happen since we're serializing keys directly from `ECPublicKey` instances on the way in
      throw new IllegalArgumentException(e);
    }
  }
//  private static final Logger logger = LoggerFactory.getLogger(SingleUseECPreKeyStore.class);
  @Override
  public CompletableFuture<Void> delete(final UUID identifier, final long deviceId) {
//    logger.info("delete for :" + identifier +",deviceId = " + deviceId);
    final CompletableFuture<Void> delete = super.delete(identifier, deviceId);
    delete.exceptionally(ex ->{
//      logger.error("delete for err",ex);
      throw ExceptionUtils.wrap(ex);
    });
    return delete;
  }

  @Override
  public CompletableFuture<Void> delete(final UUID identifier) {
//    logger.info("delete for :" + identifier);
    return super.delete(identifier);
  }
}
