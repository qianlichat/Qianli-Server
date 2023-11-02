/*
 * Copyright 2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.storage;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.signal.storageservice.metrics.StorageMetrics;
import org.signal.storageservice.storage.protos.groups.Group;
import org.signal.storageservice.storage.protos.groups.GroupChange;
import org.signal.storageservice.storage.protos.groups.GroupChanges.GroupChangeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.storage.AbstractDynamoDbStore;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.Pair;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static com.codahale.metrics.MetricRegistry.name;

public class GroupsManager extends AbstractDynamoDbStore {
  private static final String KEY = "G";
  public static final String ATTR_GROUP_DATA = "D";
  public static final String ATTR_VERSION    = "V";

  public static final String ATTR_CHANGE    = "C";
  public static final String ATTR_STATE     = "S";
  private static final Logger log = LoggerFactory.getLogger(GroupsManager.class);
  private final DynamoDbAsyncClient asyncClient;
  private final String groupTableName;
  private final String groupLogTableName;
  private final MetricRegistry metricRegistry      = SharedMetricRegistries.getOrCreate(StorageMetrics.NAME);
  private final Timer          getFromVersionTimer = metricRegistry.timer(name(GroupsManager.class, "getFromVersion"));

  public GroupsManager(
      final DynamoDbClient client,
      final DynamoDbAsyncClient asyncClient,
      final String groupTableName,
      final String groupLogTableName
  ) {
    super(client);
    this.asyncClient = asyncClient;
    this.groupTableName = groupTableName;
    this.groupLogTableName = groupLogTableName;
  }

  private static Optional<Group> from(Map<String, AttributeValue> item) {
    if(item == null){
      return Optional.empty();
    }
    if(item.isEmpty()){
//      log.error("get empty item when init group ");
      return Optional.empty();
    }
    if (!item.containsKey(ATTR_GROUP_DATA)) {
//      try{
//        log.error("group data is missing, id = " + item.get(KEY));
//      }catch (Throwable t){
//        log.error("group data is missing, id = NONE");
//      }
      throw new RuntimeException("item missing values");
    }
    try {
      return Optional.of(Group.parseFrom(item.get(ATTR_GROUP_DATA).b().asByteBuffer()));
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public CompletableFuture<Optional<Group>> getGroup(ByteString groupId) {
    final byte[] groupIdByteArray = groupId.toByteArray();
//    log.info("getGroup : " + toString(groupIdByteArray) +", from table :" + groupTableName);
    return asyncClient.getItem(GetItemRequest.builder()
            .tableName(groupTableName)
            .key(Map.of(KEY, AttributeValues.fromByteArray(groupIdByteArray)))
            .consistentRead(true)
            .build())
//        .thenApply(getItemResponse -> {
//          log.info("get group resp: " + getItemResponse.toString());
//          return getItemResponse;})
        .thenApply(GetItemResponse::item)
        .thenApply(GroupsManager::from);
  }

  public CompletableFuture<Boolean> createGroup(ByteString groupId, Group group) {
    final byte[] byteArray = group.toByteArray();
    final byte[] groupIdByteArray = groupId.toByteArray();
//    log.info("createGroup : " + toString(groupIdByteArray) +", data length = " + byteArray.length);
    return asyncClient.putItem(PutItemRequest.builder()
            .tableName(groupTableName)
            .item(Map.of(
                KEY, AttributeValues.fromByteArray(groupIdByteArray),
                ATTR_GROUP_DATA, AttributeValues.fromByteArray(byteArray),
                ATTR_VERSION, AttributeValues.fromInt(group.getVersion())))
        .build())
        .thenApply(ignored -> true)
        .exceptionally(throwable -> {
          if (ExceptionUtils.unwrap(throwable) instanceof ConditionalCheckFailedException) {
            return false;
          }

          throw ExceptionUtils.wrap(throwable);
        });
  }

  private String toString(byte[] a) {
    if (a == null)
      return "null";
    int iMax = a.length - 1;
    if (iMax == -1)
      return "[]";

    return Base64.getEncoder().encodeToString(a);
  }

  public CompletableFuture<Optional<Group>> updateGroup(ByteString groupId, Group group) {
    final byte[] byteArray = group.toByteArray();
    final byte[] groupIdByteArray = groupId.toByteArray();
//    log.info("updateGroup : " + toString(groupIdByteArray) +", data length = " + byteArray.length);
    return asyncClient.updateItem(UpdateItemRequest.builder()
            .tableName(groupTableName)
            .key(Map.of(KEY, AttributeValues.fromByteArray(groupIdByteArray)))
            .updateExpression("SET " + ATTR_GROUP_DATA +" = :data, " + ATTR_VERSION +" = :ver")
            .expressionAttributeValues(Map.of(
                ":data",AttributeValues.fromByteArray(byteArray),
                ":ver",AttributeValues.fromInt(group.getVersion())
            ))
            .build())
        .thenApply(resp -> {
//          log.info("updateGroup success!");
          return Optional.of(group);
        });
  }

  public CompletableFuture<List<GroupChangeState>> getChangeRecords(ByteString groupId, Group group,
      @Nullable Integer maxSupportedChangeEpoch, boolean includeFirstState, boolean includeLastState,
      int fromVersionInclusive, int toVersionExclusive) {
    if (fromVersionInclusive >= toVersionExclusive) {
      throw new IllegalArgumentException("Version to read from (" + fromVersionInclusive + ") must be less than version to read to (" + toVersionExclusive + ")");
    }
    int currentVersion = group.getVersion();

    Timer.Context timerContext = getFromVersionTimer.time();
    CompletableFuture<Pair<List<GroupChangeState>, Boolean>> future = new CompletableFuture<>();

    QueryRequest.Builder queryRequestBuilder = QueryRequest.builder()
        .tableName(groupLogTableName)
        .keyConditionExpression(KEY + " = :groupIdValue and "+ATTR_VERSION+" BETWEEN :fromVersion AND :toVersion")
        .expressionAttributeValues(Map.of(
            ":groupIdValue", AttributeValues.fromByteArray(groupId.toByteArray()),
            ":fromVersion", AttributeValues.fromInt(fromVersionInclusive),
            ":toVersion", AttributeValues.fromInt(toVersionExclusive - 1)  // 注意这里使用toVersionExclusive - 1
        ));

    final List<GroupChangeState> results = new LinkedList<>();

    asyncClient.query(queryRequestBuilder.build()).thenAccept(response -> {
      boolean seenCurrentVersion = false;
      for (Map<String, AttributeValue> item : response.items()) {
        try {
          GroupChange groupChange = GroupChange.parseFrom(item.get(ATTR_CHANGE).b().asByteBuffer());
          Group groupState = Group.parseFrom(item.get(ATTR_STATE).b().asByteBuffer());
          if (groupState.getVersion() == currentVersion) {
            seenCurrentVersion = true;
          }
          GroupChangeState.Builder groupChangeStateBuilder = GroupChangeState.newBuilder().setGroupChange(groupChange);
          if (maxSupportedChangeEpoch == null || maxSupportedChangeEpoch < groupChange.getChangeEpoch()
              || (includeFirstState && groupState.getVersion() == fromVersionInclusive)
              || (includeLastState && groupState.getVersion() == toVersionExclusive - 1)) {
            groupChangeStateBuilder.setGroupState(groupState);
          }
          results.add(groupChangeStateBuilder.build());
        } catch (InvalidProtocolBufferException e) {
          future.completeExceptionally(e);
        }
      }
      timerContext.close();
      future.complete(new Pair<>(results, seenCurrentVersion));
    }).exceptionally(e -> {
      timerContext.close();
      future.completeExceptionally(e);
      return null;
    });

    return future
        .thenApply(groupChangeStatesAndSeenCurrentVersion -> {
          List<GroupChangeState> groupChangeStates = groupChangeStatesAndSeenCurrentVersion.first();
          boolean seenCurrentVersion = groupChangeStatesAndSeenCurrentVersion.second();
          if (isGroupInRange(group, fromVersionInclusive, toVersionExclusive) && !seenCurrentVersion && toVersionExclusive - 1 == group.getVersion()) {
            groupChangeStates.add(GroupChangeState.newBuilder().setGroupState(group).build());
          }
          return groupChangeStates;
        });
  }

  public CompletableFuture<Boolean> appendChangeRecord(ByteString groupId, int version, GroupChange change, Group state) {
    Map<String, AttributeValue> item = Map.of(
        KEY, AttributeValues.fromByteArray(groupId.toByteArray()),
        ATTR_VERSION,AttributeValues.fromInt(version),
        ATTR_CHANGE, AttributeValues.fromByteArray(change.toByteArray()),
        ATTR_STATE, AttributeValues.fromByteArray(state.toByteArray())
    );

    PutItemRequest request = PutItemRequest.builder()
        .tableName(groupLogTableName)
        .item(item)
        .build();

    return asyncClient.putItem(request)
        .thenApply(response -> true)
        .exceptionally(throwable -> {
          if (ExceptionUtils.unwrap(throwable) instanceof ConditionalCheckFailedException) {
            return false;
          }

          throw ExceptionUtils.wrap(throwable);
        });
  }

  private static boolean isGroupInRange(Group group, int fromVersionInclusive, int toVersionExclusive) {
    return fromVersionInclusive <= group.getVersion() && group.getVersion() < toVersionExclusive;
  }
}
