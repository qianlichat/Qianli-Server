/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static com.codahale.metrics.MetricRegistry.name;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.auth.basic.BasicCredentials;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.time.Clock;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.RefreshingAccountAndDeviceSupplier;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.websocket.AuthenticatedConnectListener;

public class BaseAccountAuthenticator {

  private static final String AUTHENTICATION_COUNTER_NAME = name(BaseAccountAuthenticator.class, "authentication");
  private static final String ENABLED_NOT_REQUIRED_AUTHENTICATION_COUNTER_NAME = name(BaseAccountAuthenticator.class,
      "enabledNotRequiredAuthentication");
  private static final String AUTHENTICATION_SUCCEEDED_TAG_NAME = "succeeded";
  private static final String AUTHENTICATION_FAILURE_REASON_TAG_NAME = "reason";
  private static final String ENABLED_TAG_NAME = "enabled";

  private static final String DAYS_SINCE_LAST_SEEN_DISTRIBUTION_NAME = name(BaseAccountAuthenticator.class, "daysSinceLastSeen");
  private static final String IS_PRIMARY_DEVICE_TAG = "isPrimary";

  @VisibleForTesting
  static final char DEVICE_ID_SEPARATOR = '.';

  private final AccountsManager accountsManager;
  private final Clock           clock;

  public BaseAccountAuthenticator(AccountsManager accountsManager) {
    this(accountsManager, Clock.systemUTC());
  }

  @VisibleForTesting
  public BaseAccountAuthenticator(AccountsManager accountsManager, Clock clock) {
    this.accountsManager   = accountsManager;
    this.clock             = clock;
  }

  static Pair<String, Long> getIdentifierAndDeviceId(final String basicUsername) {
    final String identifier;
    final long deviceId;

    final int deviceIdSeparatorIndex = basicUsername.indexOf(DEVICE_ID_SEPARATOR);

    if (deviceIdSeparatorIndex == -1) {
      identifier = basicUsername;
      deviceId = Device.MASTER_ID;
    } else {
      identifier = basicUsername.substring(0, deviceIdSeparatorIndex);
      deviceId = Long.parseLong(basicUsername.substring(deviceIdSeparatorIndex + 1));
    }

    return new Pair<>(identifier, deviceId);
  }
  private static final Logger logger = LoggerFactory.getLogger(BaseAccountAuthenticator.class);
  public Optional<AuthenticatedAccount> authenticate(BasicCredentials basicCredentials, boolean enabledRequired) {
    logger.info("authenticate ...");
    boolean succeeded = false;
    String failureReason = null;

    try {
      final UUID accountUuid;
      final long deviceId;
      {
        final Pair<String, Long> identifierAndDeviceId = getIdentifierAndDeviceId(basicCredentials.getUsername());

        accountUuid = UUID.fromString(identifierAndDeviceId.first());
        deviceId = identifierAndDeviceId.second();
      }

      Optional<Account> account = accountsManager.getByAccountIdentifier(accountUuid);

      if (account.isEmpty()) {
        logger.info("authenticate ... noSuchAccount");
        failureReason = "noSuchAccount";
        return Optional.empty();
      }

      Optional<Device> device = account.get().getDevice(deviceId);

      if (device.isEmpty()) {
        logger.info("authenticate ... noSuchDevice");
        failureReason = "noSuchDevice";
        return Optional.empty();
      }

      if (enabledRequired) {
        final boolean deviceDisabled = !device.get().isEnabled();
        if (deviceDisabled) {
          failureReason = "deviceDisabled";
          logger.info("authenticate ... deviceDisabled");
        }

        final boolean accountDisabled = !account.get().isEnabled();
        if (accountDisabled) {
          failureReason = "accountDisabled";
          logger.info("authenticate ... accountDisabled");
        }
        if (accountDisabled || deviceDisabled) {
          return Optional.empty();
        }
      } else {
        Metrics.counter(ENABLED_NOT_REQUIRED_AUTHENTICATION_COUNTER_NAME,
                ENABLED_TAG_NAME, String.valueOf(device.get().isEnabled() && account.get().isEnabled()),
                IS_PRIMARY_DEVICE_TAG, String.valueOf(device.get().isMaster()))
            .increment();
      }

      SaltedTokenHash deviceSaltedTokenHash = device.get().getAuthTokenHash();
      if (deviceSaltedTokenHash.verify(basicCredentials.getPassword())) {
        succeeded = true;
        Account authenticatedAccount = updateLastSeen(account.get(), device.get());
        if (deviceSaltedTokenHash.getVersion() != SaltedTokenHash.CURRENT_VERSION) {
          authenticatedAccount = accountsManager.updateDeviceAuthentication(
              authenticatedAccount,
              device.get(),
              SaltedTokenHash.generateFor(basicCredentials.getPassword()));  // new credentials have current version
        }
        return Optional.of(new AuthenticatedAccount(
            new RefreshingAccountAndDeviceSupplier(authenticatedAccount, device.get().getId(), accountsManager)));
      }

      return Optional.empty();
    } catch (IllegalArgumentException | InvalidAuthorizationHeaderException iae) {
      failureReason = "invalidHeader";
      logger.info("authenticate ... failureReason");
      return Optional.empty();
    } finally {
      Tags tags = Tags.of(
          AUTHENTICATION_SUCCEEDED_TAG_NAME, String.valueOf(succeeded));

      if (StringUtils.isNotBlank(failureReason)) {
        tags = tags.and(AUTHENTICATION_FAILURE_REASON_TAG_NAME, failureReason);
      }

      Metrics.counter(AUTHENTICATION_COUNTER_NAME, tags).increment();
    }
  }

  @VisibleForTesting
  public Account updateLastSeen(Account account, Device device) {
    // compute a non-negative integer between 0 and 86400.
    long n = Util.ensureNonNegativeLong(account.getUuid().getLeastSignificantBits());
    final long lastSeenOffsetSeconds = n % ChronoUnit.DAYS.getDuration().toSeconds();

    // produce a truncated timestamp which is either today at UTC midnight
    // or yesterday at UTC midnight, based on per-user randomized offset used.
    final long todayInMillisWithOffset = Util.todayInMillisGivenOffsetFromNow(clock, Duration.ofSeconds(lastSeenOffsetSeconds).negated());

    // only update the device's last seen time when it falls behind the truncated timestamp.
    // this ensure a few things:
    //   (1) each account will only update last-seen at most once per day
    //   (2) these updates will occur throughout the day rather than all occurring at UTC midnight.
    if (device.getLastSeen() < todayInMillisWithOffset) {
      Metrics.summary(DAYS_SINCE_LAST_SEEN_DISTRIBUTION_NAME, IS_PRIMARY_DEVICE_TAG, String.valueOf(device.isMaster()))
          .record(Duration.ofMillis(todayInMillisWithOffset - device.getLastSeen()).toDays());

      return accountsManager.updateDeviceLastSeen(account, device, Util.todayInMillis(clock));
    }

    return account;
  }
}
