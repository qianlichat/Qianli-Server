/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.net.HttpHeaders;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.BasicAuthorizationHeader;
import org.whispersystems.textsecuregcm.auth.PhoneVerificationTokenManager;
import org.whispersystems.textsecuregcm.auth.RegistrationLockVerificationManager;
import org.whispersystems.textsecuregcm.entities.AccountIdentityResponse;
import org.whispersystems.textsecuregcm.entities.PhoneVerificationRequest;
import org.whispersystems.textsecuregcm.entities.RegistrationLockFailure;
import org.whispersystems.textsecuregcm.entities.RegistrationRequest;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.ReportedMessageMetricsListener;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.textsecuregcm.util.Util;

@Path("/v1/registration")
@io.swagger.v3.oas.annotations.tags.Tag(name = "Registration")
public class RegistrationController {

  private static final DistributionSummary REREGISTRATION_IDLE_DAYS_DISTRIBUTION = DistributionSummary
      .builder(name(RegistrationController.class, "reregistrationIdleDays"))
      .publishPercentiles(0.75, 0.95, 0.99, 0.999)
      .distributionStatisticExpiry(Duration.ofHours(2))
      .register(Metrics.globalRegistry);

  private static final String ACCOUNT_CREATED_COUNTER_NAME = name(RegistrationController.class, "accountCreated");
  private static final String COUNTRY_CODE_TAG_NAME = "countryCode";
  private static final String REGION_CODE_TAG_NAME = "regionCode";
  private static final String VERIFICATION_TYPE_TAG_NAME = "verification";
  private static final String ACCOUNT_ACTIVATED_TAG_NAME = "accountActivated";
  private static final String INVALID_ACCOUNT_ATTRS_COUNTER_NAME = name(RegistrationController.class, "invalidAccountAttrs");

  private final AccountsManager accounts;
  private final PhoneVerificationTokenManager phoneVerificationTokenManager;
  private final RegistrationLockVerificationManager registrationLockVerificationManager;
  private final KeysManager keysManager;
  private final RateLimiters rateLimiters;

  public RegistrationController(final AccountsManager accounts,
                                final PhoneVerificationTokenManager phoneVerificationTokenManager,
                                final RegistrationLockVerificationManager registrationLockVerificationManager,
                                final KeysManager keysManager,
                                final RateLimiters rateLimiters) {
    this.accounts = accounts;
    this.phoneVerificationTokenManager = phoneVerificationTokenManager;
    this.registrationLockVerificationManager = registrationLockVerificationManager;
    this.keysManager = keysManager;
    this.rateLimiters = rateLimiters;
  }

  private static final Logger logger = LoggerFactory.getLogger(RegistrationController.class);

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Registers an account",
  description = """
      Registers a new account or attempts to “re-register” an existing account. It is expected that a well-behaved client
      could make up to three consecutive calls to this API:
      1. gets 423 from existing registration lock \n
      2. gets 409 from device available for transfer \n
      3. success \n
      """)
  @ApiResponse(responseCode = "200", description = "The phone number associated with the authenticated account was changed successfully", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "403", description = "Verification failed for the provided Registration Recovery Password")
  @ApiResponse(responseCode = "409", description = "The caller has not explicitly elected to skip transferring data from another device, but a device transfer is technically possible")
  @ApiResponse(responseCode = "422", description = "The request did not pass validation")
  @ApiResponse(responseCode = "423", content = @Content(schema = @Schema(implementation = RegistrationLockFailure.class)))
  @ApiResponse(responseCode = "429", description = "Too many attempts", headers = @Header(
      name = "Retry-After",
      description = "If present, an positive integer indicating the number of seconds before a subsequent attempt could succeed"))
  public AccountIdentityResponse register(
      @HeaderParam(HttpHeaders.AUTHORIZATION) @NotNull final BasicAuthorizationHeader authorizationHeader,
      @HeaderParam(HeaderUtils.X_SIGNAL_AGENT) final String signalAgent,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent,
      @NotNull @Valid final RegistrationRequest registrationRequest) throws RateLimitExceededException, InterruptedException {

    final String number = authorizationHeader.getUsername();
    final String password = authorizationHeader.getPassword();

    logger.info("register number="+number+", password="+password);

    RateLimiter.adaptLegacyException(() -> rateLimiters.getRegistrationLimiter().validate(number));
    if (!AccountsManager.validNewAccountAttributes(registrationRequest.accountAttributes())) {
      Metrics.counter(INVALID_ACCOUNT_ATTRS_COUNTER_NAME, Tags.of(UserAgentTagUtil.getPlatformTag(userAgent))).increment();
      throw new WebApplicationException(Response.status(422, "account attributes invalid").build());
    }
    logger.info("register number="+number+", password="+password+", here1");
    final PhoneVerificationRequest.VerificationType verificationType = phoneVerificationTokenManager.verify(number,
        registrationRequest);
    logger.info("register number="+number+", password="+password+", here2");
    final Optional<Account> existingAccount = accounts.getByE164(number);
    logger.info("register number="+number+", password="+password+", here3");
    existingAccount.ifPresent(account -> {
      final Instant accountLastSeen = Instant.ofEpochMilli(account.getLastSeen());
      final Duration timeSinceLastSeen = Duration.between(accountLastSeen, Instant.now());
      REREGISTRATION_IDLE_DAYS_DISTRIBUTION.record(timeSinceLastSeen.toDays());
    });
    logger.info("register number="+number+", password="+password+", here4");
    if (!registrationRequest.skipDeviceTransfer() && existingAccount.map(Account::isTransferSupported).orElse(false)) {
      // If a device transfer is possible, clients must explicitly opt out of a transfer (i.e. after prompting the user)
      // before we'll let them create a new account "from scratch"
      throw new WebApplicationException(Response.status(409, "device transfer available").build());
    }

    if (existingAccount.isPresent()) {
      logger.info("register number="+number+", password="+password+", here5");
      registrationLockVerificationManager.verifyRegistrationLock(existingAccount.get(),
          registrationRequest.accountAttributes().getRegistrationLock(),
          userAgent, RegistrationLockVerificationManager.Flow.REGISTRATION, verificationType);
    }
    logger.info("register number="+number+", password="+password+", here6");
    Account account = accounts.create(number, password, signalAgent, registrationRequest.accountAttributes(),
        existingAccount.map(Account::getBadges).orElseGet(ArrayList::new));
    logger.info("register number="+number+", password="+password+", here7");
    // If the request includes all the information we need to fully "activate" the account, we should do so
    if (registrationRequest.supportsAtomicAccountCreation()) {
      logger.info("register number="+number+", password="+password+", here8");
      assert registrationRequest.aciIdentityKey().isPresent();
      assert registrationRequest.pniIdentityKey().isPresent();
      assert registrationRequest.deviceActivationRequest().aciSignedPreKey().isPresent();
      assert registrationRequest.deviceActivationRequest().pniSignedPreKey().isPresent();
      assert registrationRequest.deviceActivationRequest().aciPqLastResortPreKey().isPresent();
      assert registrationRequest.deviceActivationRequest().pniPqLastResortPreKey().isPresent();
      logger.info("register number="+number+", password="+password+", here9");
      account = accounts.update(account, a -> {
        a.setIdentityKey(registrationRequest.aciIdentityKey().get());
        a.setPhoneNumberIdentityKey(registrationRequest.pniIdentityKey().get());

        final Device device = a.getMasterDevice().orElseThrow();

        device.setSignedPreKey(registrationRequest.deviceActivationRequest().aciSignedPreKey().get());
        device.setPhoneNumberIdentitySignedPreKey(registrationRequest.deviceActivationRequest().pniSignedPreKey().get());
        logger.info("register number="+number+", password="+password+", here9.1");
        registrationRequest.deviceActivationRequest().apnToken().ifPresent(apnRegistrationId -> {
          device.setApnId(apnRegistrationId.apnRegistrationId());
          device.setVoipApnId(apnRegistrationId.voipRegistrationId());
        });

        registrationRequest.deviceActivationRequest().gcmToken().ifPresent(gcmRegistrationId ->
            device.setGcmId(gcmRegistrationId.gcmRegistrationId()));
        logger.info("register number="+number+", password="+password+", here9.2");

        CompletableFuture.allOf(
                keysManager.storeEcSignedPreKeys(a.getUuid(),
                    Map.of(Device.MASTER_ID, registrationRequest.deviceActivationRequest().aciSignedPreKey().get())),
                keysManager.storePqLastResort(a.getUuid(),
                    Map.of(Device.MASTER_ID, registrationRequest.deviceActivationRequest().aciPqLastResortPreKey().get())),
                keysManager.storeEcSignedPreKeys(a.getPhoneNumberIdentifier(),
                    Map.of(Device.MASTER_ID, registrationRequest.deviceActivationRequest().pniSignedPreKey().get())),
                keysManager.storePqLastResort(a.getPhoneNumberIdentifier(),
                    Map.of(Device.MASTER_ID, registrationRequest.deviceActivationRequest().pniPqLastResortPreKey().get())))
            .join();
        logger.info("register number="+number+", password="+password+", here9.3");
      });
      logger.info("register number="+number+", password="+password+", here10");
    }

    logger.info("register number="+number+", password="+password+", here11");
    Metrics.counter(ACCOUNT_CREATED_COUNTER_NAME, Tags.of(UserAgentTagUtil.getPlatformTag(userAgent),
            Tag.of(COUNTRY_CODE_TAG_NAME, Util.getCountryCode(number)),
            Tag.of(REGION_CODE_TAG_NAME, Util.getRegion(number)),
            Tag.of(VERIFICATION_TYPE_TAG_NAME, verificationType.name()),
            Tag.of(ACCOUNT_ACTIVATED_TAG_NAME, String.valueOf(account.isEnabled()))))
        .increment();

    logger.info("register number="+number+", password="+password+", here12");
    return new AccountIdentityResponse(account.getUuid(),
        account.getNumber(),
        account.getPhoneNumberIdentifier(),
        account.getUsernameHash().orElse(null),
        existingAccount.map(Account::isStorageSupported).orElse(false));
  }

}
