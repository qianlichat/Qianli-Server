/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.controllers.RegistrationController;
import org.whispersystems.textsecuregcm.util.AsyncTimerUtil;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Scheduler;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.TransactionConflictException;
import software.amazon.awssdk.services.dynamodb.model.Update;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.utils.CompletableFutureUtils;

/**
 * "Accounts" DDB table's structure doesn't match 1:1 the {@link Account} class: most of the class fields are serialized
 * and stored in the {@link Accounts#ATTR_ACCOUNT_DATA} attribute, however there are certain fields that are stored only as DDB attributes
 * (e.g. if indexing or lookup by field is required), and there are also fields that stored in both places.
 * This class contains all the logic that decides whether or not a field of the {@link Account} class should be
 * added as an attribute, serialized as a part of {@link Accounts#ATTR_ACCOUNT_DATA}, or both. To skip serialization,
 * make sure attribute name is listed in {@link Accounts#ACCOUNT_FIELDS_TO_EXCLUDE_FROM_SERIALIZATION}. If serialization is skipped,
 * make sure the field is stored in a DDB attribute and then put back into the account object in {@link Accounts#fromItem(Map)}.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class Accounts extends AbstractDynamoDbStore {

  private static final Logger log = LoggerFactory.getLogger(Accounts.class);

  static final List<String> ACCOUNT_FIELDS_TO_EXCLUDE_FROM_SERIALIZATION = List.of("uuid", "usernameLinkHandle");

  private static final ObjectWriter ACCOUNT_DDB_JSON_WRITER = SystemMapper.jsonMapper()
      .writer(SystemMapper.excludingField(Account.class, ACCOUNT_FIELDS_TO_EXCLUDE_FROM_SERIALIZATION));

  private static final Timer CREATE_TIMER = Metrics.timer(name(Accounts.class, "create"));
  private static final Timer CHANGE_NUMBER_TIMER = Metrics.timer(name(Accounts.class, "changeNumber"));
  private static final Timer SET_USERNAME_TIMER = Metrics.timer(name(Accounts.class, "setUsername"));
  private static final Timer RESERVE_USERNAME_TIMER = Metrics.timer(name(Accounts.class, "reserveUsername"));
  private static final Timer CLEAR_USERNAME_HASH_TIMER = Metrics.timer(name(Accounts.class, "clearUsernameHash"));
  private static final Timer UPDATE_TIMER = Metrics.timer(name(Accounts.class, "update"));
  private static final Timer GET_BY_NUMBER_TIMER = Metrics.timer(name(Accounts.class, "getByNumber"));
  private static final Timer GET_BY_USERNAME_HASH_TIMER = Metrics.timer(name(Accounts.class, "getByUsernameHash"));
  private static final Timer GET_BY_USERNAME_LINK_HANDLE_TIMER = Metrics.timer(name(Accounts.class, "getByUsernameLinkHandle"));
  private static final Timer GET_BY_PNI_TIMER = Metrics.timer(name(Accounts.class, "getByPni"));
  private static final Timer GET_BY_UUID_TIMER = Metrics.timer(name(Accounts.class, "getByUuid"));
  private static final Timer DELETE_TIMER = Metrics.timer(name(Accounts.class, "delete"));

  private static final String CONDITIONAL_CHECK_FAILED = "ConditionalCheckFailed";

  private static final String TRANSACTION_CONFLICT = "TransactionConflict";

  // uuid, primary key
  static final String KEY_ACCOUNT_UUID = "U";
  // uuid, attribute on account table, primary key for PNI table
  static final String ATTR_PNI_UUID = "PNI";
  static final String ATTR_PWD = "PWD";
  // uuid of the current username link or null
  static final String ATTR_USERNAME_LINK_UUID = "UL";
  // phone number
  static final String ATTR_ACCOUNT_E164 = "P";
  // account, serialized to JSON
  static final String ATTR_ACCOUNT_DATA = "D";
  // internal version for optimistic locking
  static final String ATTR_VERSION = "V";
  // canonically discoverable
  static final String ATTR_CANONICALLY_DISCOVERABLE = "C";
  // username hash; byte[] or null
  static final String ATTR_USERNAME_HASH = "N";
  // confirmed; bool
  static final String ATTR_CONFIRMED = "F";
  // unidentified access key; byte[] or null
  static final String ATTR_UAK = "UAK";
  // time to live; number
  static final String ATTR_TTL = "TTL";

  static final String DELETED_ACCOUNTS_KEY_ACCOUNT_E164 = "P";
  static final String DELETED_ACCOUNTS_ATTR_ACCOUNT_UUID = "U";
  static final String DELETED_ACCOUNTS_ATTR_EXPIRES = "E";
  static final String DELETED_ACCOUNTS_UUID_TO_E164_INDEX_NAME = "u_to_p";

  static final String USERNAME_LINK_TO_UUID_INDEX = "ul_to_u";

  static final Duration DELETED_ACCOUNTS_TIME_TO_LIVE = Duration.ofDays(30);

  private final Clock clock;

  private final DynamoDbAsyncClient asyncClient;

  private final String phoneNumberConstraintTableName;
  private final String phoneNumberIdentifierConstraintTableName;
  private final String usernamesConstraintTableName;
  private final String deletedAccountsTableName;
  private final String accountsTableName;

  @VisibleForTesting
  public Accounts(
      final Clock clock,
      final DynamoDbClient client,
      final DynamoDbAsyncClient asyncClient,
      final String accountsTableName,
      final String phoneNumberConstraintTableName,
      final String phoneNumberIdentifierConstraintTableName,
      final String usernamesConstraintTableName,
      final String deletedAccountsTableName) {
    super(client);
    this.clock = clock;
    this.asyncClient = asyncClient;
    this.phoneNumberConstraintTableName = phoneNumberConstraintTableName;
    this.phoneNumberIdentifierConstraintTableName = phoneNumberIdentifierConstraintTableName;
    this.accountsTableName = accountsTableName;
    this.usernamesConstraintTableName = usernamesConstraintTableName;
    this.deletedAccountsTableName = deletedAccountsTableName;
  }

//  private static final Logger logger = LoggerFactory.getLogger(Accounts.class);

  public Accounts(
      final DynamoDbClient client,
      final DynamoDbAsyncClient asyncClient,
      final String accountsTableName,
      final String phoneNumberConstraintTableName,
      final String phoneNumberIdentifierConstraintTableName,
      final String usernamesConstraintTableName,
      final String deletedAccountsTableName) {
    this(Clock.systemUTC(), client, asyncClient, accountsTableName,
        phoneNumberConstraintTableName, phoneNumberIdentifierConstraintTableName, usernamesConstraintTableName,
        deletedAccountsTableName);
  }

  public boolean create(final Account account) {
    return CREATE_TIMER.record(() -> {
      try {
//        logger.info("register  here6.1.1");
        final AttributeValue uuidAttr = AttributeValues.fromUUID(account.getUuid());
        final AttributeValue numberAttr = AttributeValues.fromString(account.getNumber());
        final AttributeValue pniUuidAttr = AttributeValues.fromUUID(account.getPhoneNumberIdentifier());

        final TransactWriteItem phoneNumberConstraintPut = buildConstraintTablePutIfAbsent(
            phoneNumberConstraintTableName, uuidAttr, ATTR_ACCOUNT_E164, numberAttr);
//        logger.info("register  here6.1.2");
        final TransactWriteItem phoneNumberIdentifierConstraintPut = buildConstraintTablePutIfAbsent(
            phoneNumberIdentifierConstraintTableName, uuidAttr, ATTR_PNI_UUID, pniUuidAttr);
//        logger.info("register  here6.1.3");
        final TransactWriteItem accountPut = buildAccountPut(account, uuidAttr, numberAttr, pniUuidAttr);
//        logger.info("register  here6.1.4");
        // Clear any "recently deleted account" record for this number since, if it existed, we've used its old ACI for
        // the newly-created account.
        final TransactWriteItem deletedAccountDelete = buildRemoveDeletedAccount(account.getNumber());
//        logger.info("register  here6.1.5");
        final TransactWriteItemsRequest request = TransactWriteItemsRequest.builder()
            .transactItems(phoneNumberConstraintPut, phoneNumberIdentifierConstraintPut, accountPut, deletedAccountDelete)
            .build();

//        logger.info("register  here6.1.6");

        try {
          db().transactWriteItems(request);
//          logger.info("register  here6.1.7");
        } catch (final TransactionCanceledException e) {
//          logger.info("register  here6.1.8");

          final CancellationReason accountCancellationReason = e.cancellationReasons().get(2);

          if (conditionalCheckFailed(accountCancellationReason)) {
            throw new IllegalArgumentException("account identifier present with different phone number");
          }

          final CancellationReason phoneNumberConstraintCancellationReason = e.cancellationReasons().get(0);
          final CancellationReason phoneNumberIdentifierConstraintCancellationReason = e.cancellationReasons().get(1);

          if (conditionalCheckFailed(phoneNumberConstraintCancellationReason)
              || conditionalCheckFailed(phoneNumberIdentifierConstraintCancellationReason)) {

            // In theory, both reasons should trip in tandem and either should give us the information we need. Even so,
            // we'll be cautious here and make sure we're choosing a condition check that really failed.
            final CancellationReason reason = conditionalCheckFailed(phoneNumberConstraintCancellationReason)
                ? phoneNumberConstraintCancellationReason
                : phoneNumberIdentifierConstraintCancellationReason;

            final ByteBuffer actualAccountUuid = reason.item().get(KEY_ACCOUNT_UUID).b().asByteBuffer();
            account.setUuid(UUIDUtil.fromByteBuffer(actualAccountUuid));

            final Account existingAccount = getByAccountIdentifier(account.getUuid()).orElseThrow();

            // It's up to the client to delete this username hash if they can't retrieve and decrypt the plaintext username from storage service
            existingAccount.getUsernameHash().ifPresent(account::setUsernameHash);
            account.setNumber(existingAccount.getNumber(), existingAccount.getPhoneNumberIdentifier());
            account.setVersion(existingAccount.getVersion());

            update(account);

            return false;
          }

          if (TRANSACTION_CONFLICT.equals(accountCancellationReason.code())) {
            // this should only happen if two clients manage to make concurrent create() calls
            throw new ContestedOptimisticLockException();
          }

          // this shouldn't happen
          throw new RuntimeException("could not create account: " + extractCancellationReasonCodes(e));
        } catch (Throwable t){
//          logger.error("create account failed 2:",t);
          throw t;
        }
      } catch (final JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      } catch (Throwable t){
//        logger.error("create account failed:",t);
        throw t;
      }

      return true;
    });
  }

  /**
   * Changes the phone number for the given account. The given account's number should be its current, pre-change
   * number. If this method succeeds, the account's number will be changed to the new number and its phone number
   * identifier will be changed to the given phone number identifier. If the update fails for any reason, the account's
   * number and PNI will be unchanged.
   * <p/>
   * This method expects that any accounts with conflicting numbers will have been removed by the time this method is
   * called. This method may fail with an unspecified {@link RuntimeException} if another account with the same number
   * exists in the data store.
   *
   * @param account the account for which to change the phone number
   * @param number the new phone number
   */
  public void changeNumber(final Account account,
      final String number,
      final UUID phoneNumberIdentifier,
      final Optional<UUID> maybeDisplacedAccountIdentifier) {

    CHANGE_NUMBER_TIMER.record(() -> {
      final String originalNumber = account.getNumber();
      final UUID originalPni = account.getPhoneNumberIdentifier();

      boolean succeeded = false;

      account.setNumber(number, phoneNumberIdentifier);

      try {
        final List<TransactWriteItem> writeItems = new ArrayList<>();
        final AttributeValue uuidAttr = AttributeValues.fromUUID(account.getUuid());
        final AttributeValue numberAttr = AttributeValues.fromString(number);
        final AttributeValue pniAttr = AttributeValues.fromUUID(phoneNumberIdentifier);

        writeItems.add(buildDelete(phoneNumberConstraintTableName, ATTR_ACCOUNT_E164, originalNumber));
        writeItems.add(buildConstraintTablePut(phoneNumberConstraintTableName, uuidAttr, ATTR_ACCOUNT_E164, numberAttr));
        writeItems.add(buildDelete(phoneNumberIdentifierConstraintTableName, ATTR_PNI_UUID, originalPni));
        writeItems.add(buildConstraintTablePut(phoneNumberIdentifierConstraintTableName, uuidAttr, ATTR_PNI_UUID, pniAttr));
        writeItems.add(buildRemoveDeletedAccount(number));
        maybeDisplacedAccountIdentifier.ifPresent(displacedAccountIdentifier ->
            writeItems.add(buildPutDeletedAccount(displacedAccountIdentifier, originalNumber)));

        writeItems.add(
            TransactWriteItem.builder()
                .update(Update.builder()
                    .tableName(accountsTableName)
                    .key(Map.of(KEY_ACCOUNT_UUID, uuidAttr))
                    .updateExpression(
                        "SET #data = :data, #number = :number, #pni = :pni, #cds = :cds ADD #version :version_increment")
                    .conditionExpression(
                        "attribute_exists(#number) AND #version = :version")
                    .expressionAttributeNames(Map.of(
                        "#number", ATTR_ACCOUNT_E164,
                        "#data", ATTR_ACCOUNT_DATA,
                        "#cds", ATTR_CANONICALLY_DISCOVERABLE,
                        "#pni", ATTR_PNI_UUID,
                        "#version", ATTR_VERSION))
                    .expressionAttributeValues(Map.of(
                        ":number", numberAttr,
                        ":data", accountDataAttributeValue(account),
                        ":cds", AttributeValues.fromBool(account.shouldBeVisibleInDirectory()),
                        ":pni", pniAttr,
                        ":version", AttributeValues.fromInt(account.getVersion()),
                        ":version_increment", AttributeValues.fromInt(1)))
                    .build())
                .build());

        final TransactWriteItemsRequest request = TransactWriteItemsRequest.builder()
            .transactItems(writeItems)
            .build();

        db().transactWriteItems(request);

        account.setVersion(account.getVersion() + 1);
        succeeded = true;
      } catch (final JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      } finally {
        if (!succeeded) {
          account.setNumber(originalNumber, originalPni);
        }
      }
    });
  }

  /**
   * Reserve a username hash under the account UUID
   */
  public CompletableFuture<Void> reserveUsernameHash(
      final Account account,
      final byte[] reservedUsernameHash,
      final Duration ttl) {

    final Timer.Sample sample = Timer.start();

    // if there is an existing old reservation it will be cleaned up via ttl
    final Optional<byte[]> maybeOriginalReservation = account.getReservedUsernameHash();
    account.setReservedUsernameHash(reservedUsernameHash);

    final long expirationTime = clock.instant().plus(ttl).getEpochSecond();

    // Use account UUID as a "reservation token" - by providing this, the client proves ownership of the hash
    final UUID uuid = account.getUuid();
    final byte[] accountJsonBytes;

    try {
      accountJsonBytes = SystemMapper.jsonMapper().writeValueAsBytes(account);
    } catch (final JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }

    final List<TransactWriteItem> writeItems = new ArrayList<>();

    writeItems.add(TransactWriteItem.builder()
        .put(Put.builder()
            .tableName(usernamesConstraintTableName)
            .item(Map.of(
                KEY_ACCOUNT_UUID, AttributeValues.fromUUID(uuid),
                ATTR_USERNAME_HASH, AttributeValues.fromByteArray(reservedUsernameHash),
                ATTR_TTL, AttributeValues.fromLong(expirationTime),
                ATTR_CONFIRMED, AttributeValues.fromBool(false)))
            .conditionExpression("attribute_not_exists(#username_hash) OR (#ttl < :now)")
            .expressionAttributeNames(Map.of("#username_hash", ATTR_USERNAME_HASH, "#ttl", ATTR_TTL))
            .expressionAttributeValues(Map.of(":now", AttributeValues.fromLong(clock.instant().getEpochSecond())))
            .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
            .build())
        .build());

    writeItems.add(
        TransactWriteItem.builder()
            .update(Update.builder()
                .tableName(accountsTableName)
                .key(Map.of(KEY_ACCOUNT_UUID, AttributeValues.fromUUID(uuid)))
                .updateExpression("SET #data = :data ADD #version :version_increment")
                .conditionExpression("#version = :version")
                .expressionAttributeNames(Map.of("#data", ATTR_ACCOUNT_DATA, "#version", ATTR_VERSION))
                .expressionAttributeValues(Map.of(
                    ":data", AttributeValues.fromByteArray(accountJsonBytes),
                    ":version", AttributeValues.fromInt(account.getVersion()),
                    ":version_increment", AttributeValues.fromInt(1)))
                .build())
            .build());

    return asyncClient.transactWriteItems(TransactWriteItemsRequest.builder()
            .transactItems(writeItems)
            .build())
        .exceptionally(throwable -> {
          if (ExceptionUtils.unwrap(throwable) instanceof TransactionCanceledException e) {
            if (e.cancellationReasons().stream().map(CancellationReason::code).anyMatch(CONDITIONAL_CHECK_FAILED::equals)) {
              throw new ContestedOptimisticLockException();
            }
          }

          throw ExceptionUtils.wrap(throwable);
        })
        .whenComplete((response, throwable) -> {
          sample.stop(RESERVE_USERNAME_TIMER);

          if (throwable == null) {
            account.setVersion(account.getVersion() + 1);
          } else {
            account.setReservedUsernameHash(maybeOriginalReservation.orElse(null));
          }
        })
        .thenRun(() -> {});
  }

  /**
   * Confirm (set) a previously reserved username hash
   *
   * @param account to update
   * @param usernameHash believed to be available
   * @return a future that completes once the username hash has been confirmed; may fail with an
   * {@link ContestedOptimisticLockException} if the account has been updated or the username has taken by someone else
   */
  public CompletableFuture<Void> confirmUsernameHash(final Account account, final byte[] usernameHash, @Nullable final byte[] encryptedUsername) {
    final Timer.Sample sample = Timer.start();
    final UUID newLinkHandle = UUID.randomUUID();

    final TransactWriteItemsRequest request;

    try {
      final Account updatedAccount = AccountUtil.cloneAccountAsNotStale(account);
      updatedAccount.setUsernameHash(usernameHash);
      updatedAccount.setReservedUsernameHash(null);
      updatedAccount.setUsernameLinkDetails(encryptedUsername == null ? null : newLinkHandle, encryptedUsername);

      request = buildConfirmUsernameHashRequest(updatedAccount, account.getUsernameHash());
    } catch (final JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }

    return asyncClient.transactWriteItems(request)
        .thenRun(() -> {
          account.setUsernameHash(usernameHash);
          account.setReservedUsernameHash(null);
          account.setUsernameLinkDetails(encryptedUsername == null ? null : newLinkHandle, encryptedUsername);

          account.setVersion(account.getVersion() + 1);
        })
        .exceptionally(throwable -> {
          if (ExceptionUtils.unwrap(throwable) instanceof TransactionCanceledException transactionCanceledException) {
            if (transactionCanceledException.cancellationReasons().stream().map(CancellationReason::code).anyMatch(CONDITIONAL_CHECK_FAILED::equals)) {
              throw new ContestedOptimisticLockException();
            }
          }

          throw ExceptionUtils.wrap(throwable);
        })
        .whenComplete((ignored, throwable) -> sample.stop(SET_USERNAME_TIMER));
  }

  private TransactWriteItemsRequest buildConfirmUsernameHashRequest(final Account updatedAccount, final Optional<byte[]> maybeOriginalUsernameHash)
      throws JsonProcessingException {

    final List<TransactWriteItem> writeItems = new ArrayList<>();
    final byte[] usernameHash = updatedAccount.getUsernameHash()
        .orElseThrow(() -> new IllegalArgumentException("Account must have a username hash"));

    // add the username hash to the constraint table, wiping out the ttl if we had already reserved the hash
    writeItems.add(TransactWriteItem.builder()
        .put(Put.builder()
            .tableName(usernamesConstraintTableName)
            .item(Map.of(
                KEY_ACCOUNT_UUID, AttributeValues.fromUUID(updatedAccount.getUuid()),
                ATTR_USERNAME_HASH, AttributeValues.fromByteArray(usernameHash),
                ATTR_CONFIRMED, AttributeValues.fromBool(true)))
            // it's not in the constraint table OR it's expired OR it was reserved by us
            .conditionExpression("attribute_not_exists(#username_hash) OR #ttl < :now OR (#aci = :aci AND #confirmed = :confirmed)")
            .expressionAttributeNames(Map.of("#username_hash", ATTR_USERNAME_HASH, "#ttl", ATTR_TTL, "#aci", KEY_ACCOUNT_UUID, "#confirmed", ATTR_CONFIRMED))
            .expressionAttributeValues(Map.of(
                ":now", AttributeValues.fromLong(clock.instant().getEpochSecond()),
                ":aci", AttributeValues.fromUUID(updatedAccount.getUuid()),
                ":confirmed", AttributeValues.fromBool(false)))
            .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
            .build())
        .build());

    final StringBuilder updateExpr = new StringBuilder("SET #data = :data, #username_hash = :username_hash");
    final Map<String, AttributeValue> expressionAttributeValues = new HashMap<>(Map.of(
        ":data", accountDataAttributeValue(updatedAccount),
        ":username_hash", AttributeValues.fromByteArray(usernameHash),
        ":version", AttributeValues.fromInt(updatedAccount.getVersion()),
        ":version_increment", AttributeValues.fromInt(1)));
    if (updatedAccount.getUsernameLinkHandle() != null) {
      updateExpr.append(", #ul = :ul");
      expressionAttributeValues.put(":ul", AttributeValues.fromUUID(updatedAccount.getUsernameLinkHandle()));
    } else {
      updateExpr.append(" REMOVE #ul");
    }
    updateExpr.append(" ADD #version :version_increment");

    writeItems.add(
        TransactWriteItem.builder()
            .update(Update.builder()
                .tableName(accountsTableName)
                .key(Map.of(KEY_ACCOUNT_UUID, AttributeValues.fromUUID(updatedAccount.getUuid())))
                .updateExpression(updateExpr.toString())
                .conditionExpression("#version = :version")
                .expressionAttributeNames(Map.of("#data", ATTR_ACCOUNT_DATA,
                    "#username_hash", ATTR_USERNAME_HASH,
                    "#ul", ATTR_USERNAME_LINK_UUID,
                    "#version", ATTR_VERSION))
                .expressionAttributeValues(expressionAttributeValues)
                .build())
            .build());

    maybeOriginalUsernameHash.ifPresent(originalUsernameHash -> writeItems.add(
        buildDelete(usernamesConstraintTableName, ATTR_USERNAME_HASH, originalUsernameHash)));

    return TransactWriteItemsRequest.builder()
        .transactItems(writeItems)
        .build();
  }

  public CompletableFuture<Void> clearUsernameHash(final Account account) {
    return account.getUsernameHash().map(usernameHash -> {
      final Timer.Sample sample = Timer.start();

      @Nullable final UUID originalLinkHandle = account.getUsernameLinkHandle();
      @Nullable final byte[] originalEncryptedUsername = account.getEncryptedUsername().orElse(null);

      final TransactWriteItemsRequest request;

      try {
        final Account updatedAccount = AccountUtil.cloneAccountAsNotStale(account);
        updatedAccount.setUsernameHash(null);
        updatedAccount.setUsernameLinkDetails(null, null);

        request = buildClearUsernameHashRequest(updatedAccount, usernameHash);
      } catch (final JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      }

      return asyncClient.transactWriteItems(request)
          .thenAccept(ignored -> {
            account.setUsernameHash(null);
            account.setUsernameLinkDetails(null, null);

            account.setVersion(account.getVersion() + 1);
          })
          .exceptionally(throwable -> {
            if (ExceptionUtils.unwrap(throwable) instanceof TransactionCanceledException transactionCanceledException) {
              if (conditionalCheckFailed(transactionCanceledException.cancellationReasons().get(0))) {
                throw new ContestedOptimisticLockException();
              }
            }

            throw ExceptionUtils.wrap(throwable);
          })
          .whenComplete((ignored, throwable) -> sample.stop(CLEAR_USERNAME_HASH_TIMER));
    }).orElseGet(() -> CompletableFuture.completedFuture(null));
  }

  private TransactWriteItemsRequest buildClearUsernameHashRequest(final Account updatedAccount, final byte[] originalUsernameHash)
      throws JsonProcessingException {

    final List<TransactWriteItem> writeItems = new ArrayList<>();

    writeItems.add(
        TransactWriteItem.builder()
            .update(Update.builder()
                .tableName(accountsTableName)
                .key(Map.of(KEY_ACCOUNT_UUID, AttributeValues.fromUUID(updatedAccount.getUuid())))
                .updateExpression("SET #data = :data REMOVE #username_hash, #username_link ADD #version :version_increment")
                .conditionExpression("#version = :version")
                .expressionAttributeNames(Map.of("#data", ATTR_ACCOUNT_DATA,
                    "#username_hash", ATTR_USERNAME_HASH,
                    "#username_link", ATTR_USERNAME_LINK_UUID,
                    "#version", ATTR_VERSION))
                .expressionAttributeValues(Map.of(
                    ":data", accountDataAttributeValue(updatedAccount),
                    ":version", AttributeValues.fromInt(updatedAccount.getVersion()),
                    ":version_increment", AttributeValues.fromInt(1)))
                .build())
            .build());

    writeItems.add(buildDelete(usernamesConstraintTableName, ATTR_USERNAME_HASH, originalUsernameHash));

    return TransactWriteItemsRequest.builder()
        .transactItems(writeItems)
        .build();
  }

  @Nonnull
  public CompletionStage<Void> updateAsync(final Account account) {
    return AsyncTimerUtil.record(UPDATE_TIMER, () -> {
      final UpdateItemRequest updateItemRequest;
      try {
        // username, e164, and pni cannot be modified through this method
        final Map<String, String> attrNames = new HashMap<>(Map.of(
            "#number", ATTR_ACCOUNT_E164,
            "#data", ATTR_ACCOUNT_DATA,
            "#cds", ATTR_CANONICALLY_DISCOVERABLE,
            "#version", ATTR_VERSION));

        final Map<String, AttributeValue> attrValues = new HashMap<>(Map.of(
            ":data", accountDataAttributeValue(account),
            ":cds", AttributeValues.fromBool(account.shouldBeVisibleInDirectory()),
            ":version", AttributeValues.fromInt(account.getVersion()),
            ":version_increment", AttributeValues.fromInt(1)));

        final StringBuilder updateExpressionBuilder = new StringBuilder("SET #data = :data, #cds = :cds");
        if (account.getUnidentifiedAccessKey().isPresent()) {
          // if it's present in the account, also set the uak
          attrNames.put("#uak", ATTR_UAK);
          attrValues.put(":uak", AttributeValues.fromByteArray(account.getUnidentifiedAccessKey().get()));
          updateExpressionBuilder.append(", #uak = :uak");
        }
        if (account.getEncryptedUsername().isPresent() && account.getUsernameLinkHandle() != null) {
          attrNames.put("#ul", ATTR_USERNAME_LINK_UUID);
          attrValues.put(":ul", AttributeValues.fromUUID(account.getUsernameLinkHandle()));
          updateExpressionBuilder.append(", #ul = :ul");
        }
        updateExpressionBuilder.append(" ADD #version :version_increment");
        if (account.getEncryptedUsername().isEmpty() || account.getUsernameLinkHandle() == null) {
          attrNames.put("#ul", ATTR_USERNAME_LINK_UUID);
          updateExpressionBuilder.append(" REMOVE #ul");
        }

        updateItemRequest = UpdateItemRequest.builder()
            .tableName(accountsTableName)
            .key(Map.of(KEY_ACCOUNT_UUID, AttributeValues.fromUUID(account.getUuid())))
            .updateExpression(updateExpressionBuilder.toString())
            .conditionExpression("attribute_exists(#number) AND #version = :version")
            .expressionAttributeNames(attrNames)
            .expressionAttributeValues(attrValues)
            .build();
      } catch (final JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      }

      return asyncClient.updateItem(updateItemRequest)
          .thenApply(response -> {
            account.setVersion(AttributeValues.getInt(response.attributes(), "V", account.getVersion() + 1));
            return (Void) null;
          })
          .exceptionally(throwable -> {
            final Throwable unwrapped = ExceptionUtils.unwrap(throwable);
            if (unwrapped instanceof TransactionConflictException) {
              throw new ContestedOptimisticLockException();
            } else if (unwrapped instanceof ConditionalCheckFailedException e) {
              // the exception doesn't give details about which condition failed,
              // but we can infer it was an optimistic locking failure if the UUID is known
              throw getByAccountIdentifier(account.getUuid()).isPresent() ? new ContestedOptimisticLockException() : e;
            } else {
              // rethrow
              throw CompletableFutureUtils.errorAsCompletionException(throwable);
            }
          });
    });
  }

  public void update(final Account account) throws ContestedOptimisticLockException {
    try {
      updateAsync(account).toCompletableFuture().join();
    } catch (final CompletionException e) {
      // unwrap CompletionExceptions, throw as long is it's unchecked
      Throwables.throwIfUnchecked(ExceptionUtils.unwrap(e));

      // if we otherwise somehow got a wrapped checked exception,
      // rethrow the checked exception wrapped by the original CompletionException
      log.error("Unexpected checked exception thrown from dynamo update", e);
      throw e;
    }
  }

  public CompletableFuture<Boolean> usernameHashAvailable(final byte[] username) {
    return usernameHashAvailable(Optional.empty(), username);
  }

  public CompletableFuture<Boolean> usernameHashAvailable(final Optional<UUID> accountUuid, final byte[] usernameHash) {
    return itemByKeyAsync(usernamesConstraintTableName, ATTR_USERNAME_HASH, AttributeValues.fromByteArray(usernameHash))
        .thenApply(maybeUsernameHashItem -> maybeUsernameHashItem
            .map(item -> {
              if (AttributeValues.getLong(item, ATTR_TTL, Long.MAX_VALUE) < clock.instant().getEpochSecond()) {
                // username hash was reserved, but has expired
                return true;
              }

              // username hash is reserved by us
              return !AttributeValues.getBool(item, ATTR_CONFIRMED, true) && accountUuid
                  .map(AttributeValues.getUUID(item, KEY_ACCOUNT_UUID, new UUID(0, 0))::equals)
                  .orElse(false);
            })
            // If no item was found, then the username hash is free
            .orElse(true));
  }

  @Nonnull
  public Optional<Account> getByE164(final String number) {
    return getByIndirectLookup(
        GET_BY_NUMBER_TIMER, phoneNumberConstraintTableName, ATTR_ACCOUNT_E164, AttributeValues.fromString(number));
  }

  @Nonnull
  public CompletableFuture<Optional<Account>> getByE164Async(final String number) {
    return getByIndirectLookupAsync(
        GET_BY_NUMBER_TIMER, phoneNumberConstraintTableName, ATTR_ACCOUNT_E164, AttributeValues.fromString(number));
  }

  @Nonnull
  public Optional<Account> getByPhoneNumberIdentifier(final UUID phoneNumberIdentifier) {
    return getByIndirectLookup(
        GET_BY_PNI_TIMER, phoneNumberIdentifierConstraintTableName, ATTR_PNI_UUID, AttributeValues.fromUUID(phoneNumberIdentifier));
  }

  @Nonnull
  public CompletableFuture<Optional<Account>> getByPhoneNumberIdentifierAsync(final UUID phoneNumberIdentifier) {
    return getByIndirectLookupAsync(GET_BY_PNI_TIMER, phoneNumberIdentifierConstraintTableName, ATTR_PNI_UUID, AttributeValues.fromUUID(phoneNumberIdentifier));
  }

  @Nonnull
  public CompletableFuture<Optional<Account>> getByUsernameHash(final byte[] usernameHash) {
    return getByIndirectLookupAsync(GET_BY_USERNAME_HASH_TIMER,
        usernamesConstraintTableName,
        ATTR_USERNAME_HASH,
        AttributeValues.fromByteArray(usernameHash),
        item -> AttributeValues.getBool(item, ATTR_CONFIRMED, false) // ignore items that are reservations (not confirmed)
    );
  }

  @Nonnull
  public CompletableFuture<Optional<Account>> getByUsernameLinkHandle(final UUID usernameLinkHandle) {
    final Timer.Sample sample = Timer.start();

    return itemByGsiKeyAsync(accountsTableName, USERNAME_LINK_TO_UUID_INDEX, ATTR_USERNAME_LINK_UUID, AttributeValues.fromUUID(usernameLinkHandle))
        .thenApply(maybeItem -> maybeItem.map(Accounts::fromItem))
        .whenComplete((account, throwable) -> sample.stop(GET_BY_USERNAME_LINK_HANDLE_TIMER));
  }

  @Nonnull
  public Optional<Account> getByAccountIdentifier(final UUID uuid) {
    return requireNonNull(GET_BY_UUID_TIMER.record(() ->
        itemByKey(accountsTableName, KEY_ACCOUNT_UUID, AttributeValues.fromUUID(uuid))
            .map(Accounts::fromItem)));
  }

  private TransactWriteItem buildPutDeletedAccount(final UUID uuid, final String e164) {
    return TransactWriteItem.builder()
        .put(Put.builder()
            .tableName(deletedAccountsTableName)
            .item(Map.of(
                DELETED_ACCOUNTS_KEY_ACCOUNT_E164, AttributeValues.fromString(e164),
                DELETED_ACCOUNTS_ATTR_ACCOUNT_UUID, AttributeValues.fromUUID(uuid),
                DELETED_ACCOUNTS_ATTR_EXPIRES, AttributeValues.fromLong(Instant.now().plus(DELETED_ACCOUNTS_TIME_TO_LIVE).getEpochSecond())))
            .build())
        .build();
  }

  private TransactWriteItem buildRemoveDeletedAccount(final String e164) {
    return TransactWriteItem.builder()
        .delete(Delete.builder()
            .tableName(deletedAccountsTableName)
            .key(Map.of(DELETED_ACCOUNTS_KEY_ACCOUNT_E164, AttributeValues.fromString(e164)))
            .build())
        .build();
  }

  @Nonnull
  public CompletableFuture<Optional<Account>> getByAccountIdentifierAsync(final UUID uuid) {
    return AsyncTimerUtil.record(GET_BY_UUID_TIMER, () -> itemByKeyAsync(accountsTableName, KEY_ACCOUNT_UUID, AttributeValues.fromUUID(uuid))
        .thenApply(maybeItem -> maybeItem.map(Accounts::fromItem)))
        .toCompletableFuture();
  }

  public Optional<UUID> findRecentlyDeletedAccountIdentifier(final String e164) {
    final GetItemResponse response = db().getItem(GetItemRequest.builder()
        .tableName(deletedAccountsTableName)
        .consistentRead(true)
        .key(Map.of(DELETED_ACCOUNTS_KEY_ACCOUNT_E164, AttributeValues.fromString(e164)))
        .build());

    return Optional.ofNullable(AttributeValues.getUUID(response.item(), DELETED_ACCOUNTS_ATTR_ACCOUNT_UUID, null));
  }

  public Optional<String> findRecentlyDeletedE164(final UUID uuid) {
    final QueryResponse response = db().query(QueryRequest.builder()
        .tableName(deletedAccountsTableName)
        .indexName(DELETED_ACCOUNTS_UUID_TO_E164_INDEX_NAME)
        .keyConditionExpression("#uuid = :uuid")
        .projectionExpression("#e164")
        .expressionAttributeNames(Map.of("#uuid", DELETED_ACCOUNTS_ATTR_ACCOUNT_UUID,
            "#e164", DELETED_ACCOUNTS_KEY_ACCOUNT_E164))
        .expressionAttributeValues(Map.of(":uuid", AttributeValues.fromUUID(uuid))).build());

    if (response.count() == 0) {
      return Optional.empty();
    }

    if (response.count() > 1) {
      throw new RuntimeException("Impossible result: more than one phone number returned for UUID: " + uuid);
    }

    return Optional.ofNullable(response.items().get(0).get(DELETED_ACCOUNTS_KEY_ACCOUNT_E164).s());
  }

  public CompletableFuture<Void> delete(final UUID uuid) {
    final Timer.Sample sample = Timer.start();

    return getByAccountIdentifierAsync(uuid)
        .thenCompose(maybeAccount -> maybeAccount.map(account -> {
              final List<TransactWriteItem> transactWriteItems = new ArrayList<>(List.of(
                  buildDelete(phoneNumberConstraintTableName, ATTR_ACCOUNT_E164, account.getNumber()),
                  buildDelete(accountsTableName, KEY_ACCOUNT_UUID, uuid),
                  buildDelete(phoneNumberIdentifierConstraintTableName, ATTR_PNI_UUID, account.getPhoneNumberIdentifier()),
                  buildPutDeletedAccount(uuid, account.getNumber())
              ));

              account.getUsernameHash().ifPresent(usernameHash -> transactWriteItems.add(
                  buildDelete(usernamesConstraintTableName, ATTR_USERNAME_HASH, usernameHash)));

              return asyncClient.transactWriteItems(TransactWriteItemsRequest.builder()
                  .transactItems(transactWriteItems)
                  .build())
                  .thenRun(Util.NOOP);
            })
            .orElseGet(() -> CompletableFuture.completedFuture(null)))
            .thenRun(() -> sample.stop(DELETE_TIMER));
  }

  ParallelFlux<Account> getAll(final int segments, final Scheduler scheduler) {
    if (segments < 1) {
      throw new IllegalArgumentException("Total number of segments must be positive");
    }

    return Flux.range(0, segments)
        .parallel()
        .runOn(scheduler)
        .flatMap(segment -> asyncClient.scanPaginator(ScanRequest.builder()
                .tableName(accountsTableName)
                .consistentRead(true)
                .segment(segment)
                .totalSegments(segments)
                .build())
            .items()
            .map(Accounts::fromItem));
  }

  @Nonnull
  private Optional<Account> getByIndirectLookup(
      final Timer timer,
      final String tableName,
      final String keyName,
      final AttributeValue keyValue) {
    return getByIndirectLookup(timer, tableName, keyName, keyValue, i -> true);
  }

  @Nonnull
  private CompletableFuture<Optional<Account>> getByIndirectLookupAsync(
      final Timer timer,
      final String tableName,
      final String keyName,
      final AttributeValue keyValue) {

    return getByIndirectLookupAsync(timer, tableName, keyName, keyValue, i -> true);
  }

  @Nonnull
  private Optional<Account> getByIndirectLookup(
      final Timer timer,
      final String tableName,
      final String keyName,
      final AttributeValue keyValue,
      final Predicate<? super Map<String, AttributeValue>> predicate) {

    return requireNonNull(timer.record(() -> itemByKey(tableName, keyName, keyValue)
        .filter(predicate)
        .map(item -> item.get(KEY_ACCOUNT_UUID))
        .flatMap(uuid -> itemByKey(accountsTableName, KEY_ACCOUNT_UUID, uuid))
        .map(Accounts::fromItem)));
  }

  @Nonnull
  private CompletableFuture<Optional<Account>> getByIndirectLookupAsync(
      final Timer timer,
      final String tableName,
      final String keyName,
      final AttributeValue keyValue,
      final Predicate<? super Map<String, AttributeValue>> predicate) {

    return AsyncTimerUtil.record(timer, () -> itemByKeyAsync(tableName, keyName, keyValue)
        .thenCompose(maybeItem -> maybeItem
            .filter(predicate)
            .map(item -> item.get(KEY_ACCOUNT_UUID))
            .map(uuid -> itemByKeyAsync(accountsTableName, KEY_ACCOUNT_UUID, uuid)
                .thenApply(maybeAccountItem -> maybeAccountItem.map(Accounts::fromItem)))
            .orElse(CompletableFuture.completedFuture(Optional.empty()))))
        .toCompletableFuture();
  }

  @Nonnull
  private Optional<Map<String, AttributeValue>> itemByKey(final String table, final String keyName, final AttributeValue keyValue) {
    final GetItemResponse response = db().getItem(GetItemRequest.builder()
        .tableName(table)
        .key(Map.of(keyName, keyValue))
        .consistentRead(true)
        .build());
    return Optional.ofNullable(response.item()).filter(m -> !m.isEmpty());
  }

  @Nonnull
  private CompletableFuture<Optional<Map<String, AttributeValue>>> itemByKeyAsync(final String table, final String keyName, final AttributeValue keyValue) {
    return asyncClient.getItem(GetItemRequest.builder()
            .tableName(table)
            .key(Map.of(keyName, keyValue))
            .consistentRead(true)
            .build())
        .thenApply(response -> Optional.ofNullable(response.item()).filter(item -> !item.isEmpty()));
  }

  @Nonnull
  private Optional<Map<String, AttributeValue>> itemByGsiKey(final String table, final String indexName, final String keyName, final AttributeValue keyValue) {
    final QueryResponse response = db().query(QueryRequest.builder()
        .tableName(table)
        .indexName(indexName)
        .keyConditionExpression("#gsiKey = :gsiValue")
        .projectionExpression("#uuid")
        .expressionAttributeNames(Map.of(
            "#gsiKey", keyName,
            "#uuid", KEY_ACCOUNT_UUID))
        .expressionAttributeValues(Map.of(
            ":gsiValue", keyValue))
        .build());

    if (response.count() == 0) {
      return Optional.empty();
    }

    if (response.count() > 1) {
      throw new IllegalStateException("More than one row located for GSI [%s], key-value pair [%s, %s]"
          .formatted(indexName, keyName, keyValue));
    }

    final AttributeValue primaryKeyValue = response.items().get(0).get(KEY_ACCOUNT_UUID);
    return itemByKey(table, KEY_ACCOUNT_UUID, primaryKeyValue);
  }

  @Nonnull
  private CompletableFuture<Optional<Map<String, AttributeValue>>> itemByGsiKeyAsync(final String table, final String indexName, final String keyName, final AttributeValue keyValue) {
    return asyncClient.query(QueryRequest.builder()
        .tableName(table)
        .indexName(indexName)
        .keyConditionExpression("#gsiKey = :gsiValue")
        .projectionExpression("#uuid")
        .expressionAttributeNames(Map.of(
            "#gsiKey", keyName,
            "#uuid", KEY_ACCOUNT_UUID))
        .expressionAttributeValues(Map.of(
            ":gsiValue", keyValue))
        .build())
        .thenCompose(response -> {
          if (response.count() == 0) {
            return CompletableFuture.completedFuture(Optional.empty());
          }

          if (response.count() > 1) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                "More than one row located for GSI [%s], key-value pair [%s, %s]"
                    .formatted(indexName, keyName, keyValue)));
          }

          final AttributeValue primaryKeyValue = response.items().get(0).get(KEY_ACCOUNT_UUID);
          return itemByKeyAsync(table, KEY_ACCOUNT_UUID, primaryKeyValue);
        });
  }

  @Nonnull
  private TransactWriteItem buildAccountPut(
      final Account account,
      final AttributeValue uuidAttr,
      final AttributeValue numberAttr,
      final AttributeValue pniUuidAttr) throws JsonProcessingException {

    final Map<String, AttributeValue> item = new HashMap<>(Map.of(
        KEY_ACCOUNT_UUID, uuidAttr,
        ATTR_PWD, AttributeValue.fromS(account.getPwd()),
        ATTR_ACCOUNT_E164, numberAttr,
        ATTR_PNI_UUID, pniUuidAttr,
        ATTR_ACCOUNT_DATA, accountDataAttributeValue(account),
        ATTR_VERSION, AttributeValues.fromInt(account.getVersion()),
        ATTR_CANONICALLY_DISCOVERABLE, AttributeValues.fromBool(account.shouldBeVisibleInDirectory())));

    // Add the UAK if it's in the account
    account.getUnidentifiedAccessKey()
        .map(AttributeValues::fromByteArray)
        .ifPresent(uak -> item.put(ATTR_UAK, uak));

    return TransactWriteItem.builder()
        .put(Put.builder()
            .conditionExpression("attribute_not_exists(#number) OR #number = :number")
            .expressionAttributeNames(Map.of("#number", ATTR_ACCOUNT_E164))
            .expressionAttributeValues(Map.of(":number", numberAttr))
            .tableName(accountsTableName)
            .item(item)
            .build())
        .build();
  }

  @Nonnull
  private static TransactWriteItem buildConstraintTablePutIfAbsent(
      final String tableName,
      final AttributeValue uuidAttr,
      final String keyName,
      final AttributeValue keyValue
  ) {
    return TransactWriteItem.builder()
        .put(Put.builder()
            .tableName(tableName)
            .item(Map.of(
                keyName, keyValue,
                KEY_ACCOUNT_UUID, uuidAttr))
            .conditionExpression(
                "attribute_not_exists(#key) OR #uuid = :uuid")
            .expressionAttributeNames(Map.of(
                "#key", keyName,
                "#uuid", KEY_ACCOUNT_UUID))
            .expressionAttributeValues(Map.of(
                ":uuid", uuidAttr))
            .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
            .build())
        .build();
  }

  @Nonnull
  private static TransactWriteItem buildConstraintTablePut(
      final String tableName,
      final AttributeValue uuidAttr,
      final String keyName,
      final AttributeValue keyValue) {
    return TransactWriteItem.builder()
        .put(Put.builder()
            .tableName(tableName)
            .item(Map.of(
                keyName, keyValue,
                KEY_ACCOUNT_UUID, uuidAttr))
            .conditionExpression(
                "attribute_not_exists(#key)")
            .expressionAttributeNames(Map.of(
                "#key", keyName))
            .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
            .build())
        .build();
  }

  @Nonnull
  private static TransactWriteItem buildDelete(final String tableName, final String keyName, final String keyValue) {
    return buildDelete(tableName, keyName, AttributeValues.fromString(keyValue));
  }

  @Nonnull
  private static TransactWriteItem buildDelete(final String tableName, final String keyName, final byte[] keyValue) {
    return buildDelete(tableName, keyName, AttributeValues.fromByteArray(keyValue));
  }

  @Nonnull
  private static TransactWriteItem buildDelete(final String tableName, final String keyName, final UUID keyValue) {
    return buildDelete(tableName, keyName, AttributeValues.fromUUID(keyValue));
  }

  @Nonnull
  private static TransactWriteItem buildDelete(final String tableName, final String keyName, final AttributeValue keyValue) {
    return TransactWriteItem.builder()
        .delete(Delete.builder()
            .tableName(tableName)
            .key(Map.of(keyName, keyValue))
            .build())
        .build();
  }

  @Nonnull
  private static String extractCancellationReasonCodes(final TransactionCanceledException exception) {
    return exception.cancellationReasons().stream()
        .map(CancellationReason::code)
        .collect(Collectors.joining(", "));
  }

  @VisibleForTesting
  @Nonnull
  static Account fromItem(final Map<String, AttributeValue> item) {
    if (!item.containsKey(ATTR_ACCOUNT_DATA)
        || !item.containsKey(ATTR_ACCOUNT_E164)
        || !item.containsKey(KEY_ACCOUNT_UUID)
        || !item.containsKey(ATTR_PWD)
        || !item.containsKey(ATTR_CANONICALLY_DISCOVERABLE)) {
      throw new RuntimeException("item missing values");
    }
    try {
      final Account account = SystemMapper.jsonMapper().readValue(item.get(ATTR_ACCOUNT_DATA).b().asByteArray(), Account.class);

      final UUID accountIdentifier = UUIDUtil.fromByteBuffer(item.get(KEY_ACCOUNT_UUID).b().asByteBuffer());
      final UUID phoneNumberIdentifierFromAttribute = AttributeValues.getUUID(item, ATTR_PNI_UUID, null);

      if (account.getPhoneNumberIdentifier() == null || phoneNumberIdentifierFromAttribute == null ||
          !Objects.equals(account.getPhoneNumberIdentifier(), phoneNumberIdentifierFromAttribute)) {

        log.warn("Missing or mismatched PNIs for account {}. From JSON: {}; from attribute: {}",
            accountIdentifier, account.getPhoneNumberIdentifier(), phoneNumberIdentifierFromAttribute);
      }

      account.setNumber(item.get(ATTR_ACCOUNT_E164).s(), phoneNumberIdentifierFromAttribute);
      account.setPwd(item.get(ATTR_PWD).s());
      account.setUuid(accountIdentifier);
      account.setUsernameHash(AttributeValues.getByteArray(item, ATTR_USERNAME_HASH, null));
      account.setUsernameLinkHandle(AttributeValues.getUUID(item, ATTR_USERNAME_LINK_UUID, null));
      account.setVersion(Integer.parseInt(item.get(ATTR_VERSION).n()));

      return account;

    } catch (final IOException e) {
      throw new RuntimeException("Could not read stored account data", e);
    }
  }

  private static AttributeValue accountDataAttributeValue(final Account account) throws JsonProcessingException {
    return AttributeValues.fromByteArray(ACCOUNT_DDB_JSON_WRITER.writeValueAsBytes(account));
  }

  private static boolean conditionalCheckFailed(final CancellationReason reason) {
    return CONDITIONAL_CHECK_FAILED.equals(reason.code());
  }
}
