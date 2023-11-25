/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.ServerErrorException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.codec.binary.Base32;
import org.signal.libsignal.usernames.BaseUsernameException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AccountAndAuthenticatedDeviceHolder;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.ChangesDeviceEnabledState;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.auth.TurnToken;
import org.whispersystems.textsecuregcm.auth.TurnTokenGenerator;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.AccountIdResponse;
import org.whispersystems.textsecuregcm.entities.AccountIdentifierResponse;
import org.whispersystems.textsecuregcm.entities.AccountIdentityResponse;
import org.whispersystems.textsecuregcm.entities.AccountTotpBindResponse;
import org.whispersystems.textsecuregcm.entities.AccountTotpGenResponse;
import org.whispersystems.textsecuregcm.entities.ApnRegistrationId;
import org.whispersystems.textsecuregcm.entities.ChangePasswordRequest;
import org.whispersystems.textsecuregcm.entities.ConfirmUsernameHashRequest;
import org.whispersystems.textsecuregcm.entities.DeviceName;
import org.whispersystems.textsecuregcm.entities.EncryptedUsername;
import org.whispersystems.textsecuregcm.entities.GcmRegistrationId;
import org.whispersystems.textsecuregcm.entities.RegistrationLock;
import org.whispersystems.textsecuregcm.entities.RegistrationServiceSession;
import org.whispersystems.textsecuregcm.entities.ReserveUsernameHashRequest;
import org.whispersystems.textsecuregcm.entities.ReserveUsernameHashResponse;
import org.whispersystems.textsecuregcm.entities.UsernameHashResponse;
import org.whispersystems.textsecuregcm.entities.UsernameLinkHandle;
import org.whispersystems.textsecuregcm.entities.VerificationSessionResponse;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.RateLimitedByIp;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.registration.VerificationSession;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.storage.UsernameHashNotAvailableException;
import org.whispersystems.textsecuregcm.storage.UsernameReservationNotFoundException;
import org.whispersystems.textsecuregcm.storage.VerificationSessionManager;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.textsecuregcm.util.RSAUtils;
import org.whispersystems.textsecuregcm.util.TotpUtil;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import org.whispersystems.textsecuregcm.util.UsernameHashZkProofVerifier;
import org.whispersystems.textsecuregcm.util.Util;

import static org.whispersystems.textsecuregcm.controllers.VerificationController.DYNAMODB_TIMEOUT;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/v1/accounts")
@io.swagger.v3.oas.annotations.tags.Tag(name = "Account")
public class AccountController {

  public static final int MAXIMUM_USERNAME_HASHES_LIST_LENGTH = 20;
  public static final int USERNAME_HASH_LENGTH = 32;
  public static final int MAXIMUM_USERNAME_CIPHERTEXT_LENGTH = 128;

  private final AccountsManager accounts;
  private final RateLimiters rateLimiters;
  private final TurnTokenGenerator turnTokenGenerator;
  private final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager;
  private final UsernameHashZkProofVerifier usernameHashZkProofVerifier;
  private final Logger logger = LoggerFactory.getLogger(AccountController.class);
  private final VerificationSessionManager verificationSessionManager;

  public AccountController(
      AccountsManager accounts,
      RateLimiters rateLimiters,
      TurnTokenGenerator turnTokenGenerator,
      RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager,
      UsernameHashZkProofVerifier usernameHashZkProofVerifier,
      VerificationSessionManager verificationSessionManager) {
    this.accounts = accounts;
    this.rateLimiters = rateLimiters;
    this.turnTokenGenerator = turnTokenGenerator;
    this.registrationRecoveryPasswordsManager = registrationRecoveryPasswordsManager;
    this.usernameHashZkProofVerifier = usernameHashZkProofVerifier;
    this.verificationSessionManager = verificationSessionManager;
  }

  @GET
  @Path("/turn/")
  @Produces(MediaType.APPLICATION_JSON)
  public TurnToken getTurnToken(@Auth AuthenticatedAccount auth) throws RateLimitExceededException {
    rateLimiters.getTurnLimiter().validate(auth.getAccount().getUuid());
    return turnTokenGenerator.generate(auth.getAccount().getUuid());
  }

  @PUT
  @Path("/gcm/")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ChangesDeviceEnabledState
  public void setGcmRegistrationId(@Auth DisabledPermittedAuthenticatedAccount disabledPermittedAuth,
      @NotNull @Valid GcmRegistrationId registrationId) {

    final Account account = disabledPermittedAuth.getAccount();
    final Device device = disabledPermittedAuth.getAuthenticatedDevice();

    if (Objects.equals(device.getGcmId(), registrationId.gcmRegistrationId())) {
      return;
    }

    accounts.updateDevice(account, device.getId(), d -> {
      d.setApnId(null);
      d.setVoipApnId(null);
      d.setGcmId(registrationId.gcmRegistrationId());
      d.setFetchesMessages(false);
    });
  }

  @DELETE
  @Path("/gcm/")
  @ChangesDeviceEnabledState
  public void deleteGcmRegistrationId(@Auth DisabledPermittedAuthenticatedAccount disabledPermittedAuth) {
    Account account = disabledPermittedAuth.getAccount();
    Device device = disabledPermittedAuth.getAuthenticatedDevice();

    accounts.updateDevice(account, device.getId(), d -> {
      d.setGcmId(null);
      d.setFetchesMessages(false);
      d.setUserAgent("OWA");
    });
  }

  @PUT
  @Path("/apn/")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ChangesDeviceEnabledState
  public void setApnRegistrationId(@Auth DisabledPermittedAuthenticatedAccount disabledPermittedAuth,
      @NotNull @Valid ApnRegistrationId registrationId) {

    final Account account = disabledPermittedAuth.getAccount();
    final Device device = disabledPermittedAuth.getAuthenticatedDevice();

    if (Objects.equals(device.getApnId(), registrationId.apnRegistrationId()) &&
        Objects.equals(device.getVoipApnId(), registrationId.voipRegistrationId())) {

      return;
    }

    accounts.updateDevice(account, device.getId(), d -> {
      d.setApnId(registrationId.apnRegistrationId());
      d.setVoipApnId(registrationId.voipRegistrationId());
      d.setGcmId(null);
      d.setFetchesMessages(false);
    });
  }

  @DELETE
  @Path("/apn/")
  @ChangesDeviceEnabledState
  public void deleteApnRegistrationId(@Auth DisabledPermittedAuthenticatedAccount disabledPermittedAuth) {
    Account account = disabledPermittedAuth.getAccount();
    Device device = disabledPermittedAuth.getAuthenticatedDevice();

    accounts.updateDevice(account, device.getId(), d -> {
      d.setApnId(null);
      d.setVoipApnId(null);
      d.setFetchesMessages(false);
      if (d.getId() == 1) {
        d.setUserAgent("OWI");
      } else {
        d.setUserAgent("OWP");
      }
    });
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/registration_lock")
  public void setRegistrationLock(@Auth AuthenticatedAccount auth, @NotNull @Valid RegistrationLock accountLock) {
    SaltedTokenHash credentials = SaltedTokenHash.generateFor(accountLock.getRegistrationLock());

    accounts.update(auth.getAccount(),
        a -> a.setRegistrationLock(credentials.hash(), credentials.salt()));
  }

  @DELETE
  @Path("/registration_lock")
  public void removeRegistrationLock(@Auth AuthenticatedAccount auth) {
    accounts.update(auth.getAccount(), a -> a.setRegistrationLock(null, null));
  }

  @PUT
  @Path("/name/")
  public void setName(@Auth DisabledPermittedAuthenticatedAccount disabledPermittedAuth,
      @NotNull @Valid DeviceName deviceName) {
    Account account = disabledPermittedAuth.getAccount();
    Device device = disabledPermittedAuth.getAuthenticatedDevice();
    accounts.updateDevice(account, device.getId(), d -> d.setName(deviceName.getDeviceName()));
  }

  @PUT
  @Path("/attributes/")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ChangesDeviceEnabledState
  public void setAccountAttributes(
      @Auth DisabledPermittedAuthenticatedAccount disabledPermittedAuth,
      @HeaderParam(HeaderUtils.X_SIGNAL_AGENT) String userAgent,
      @NotNull @Valid AccountAttributes attributes) {
    final Account account = disabledPermittedAuth.getAccount();
    final long deviceId = disabledPermittedAuth.getAuthenticatedDevice().getId();

    final Account updatedAccount = accounts.update(account, a -> {
      a.getDevice(deviceId).ifPresent(d -> {
        d.setFetchesMessages(attributes.getFetchesMessages());
        d.setName(attributes.getName());
        d.setLastSeen(Util.todayInMillis());
        d.setCapabilities(attributes.getCapabilities());
        d.setRegistrationId(attributes.getRegistrationId());
        attributes.getPhoneNumberIdentityRegistrationId().ifPresent(d::setPhoneNumberIdentityRegistrationId);
        d.setUserAgent(userAgent);
      });

      a.setRegistrationLockFromAttributes(attributes);
      a.setUnidentifiedAccessKey(attributes.getUnidentifiedAccessKey());
      a.setUnrestrictedUnidentifiedAccess(attributes.isUnrestrictedUnidentifiedAccess());
      a.setDiscoverableByPhoneNumber(attributes.isDiscoverableByPhoneNumber());
    });

    // if registration recovery password was sent to us, store it (or refresh its expiration)
    attributes.recoveryPassword().ifPresent(registrationRecoveryPassword ->
        registrationRecoveryPasswordsManager.storeForCurrentNumber(updatedAccount.getNumber(),
            registrationRecoveryPassword));
  }

  @GET
  @Path("/me")
  @Produces(MediaType.APPLICATION_JSON)
  public AccountIdentityResponse getMe(@Auth DisabledPermittedAuthenticatedAccount auth) {
    return buildAccountIdentityResponse(auth);
  }

  @GET
  @Path("/whoami")
  @Produces(MediaType.APPLICATION_JSON)
  public AccountIdentityResponse whoAmI(@Auth AuthenticatedAccount auth) {
    return buildAccountIdentityResponse(auth);
  }

  private AccountIdentityResponse buildAccountIdentityResponse(AccountAndAuthenticatedDeviceHolder auth) {
    return new AccountIdentityResponse(auth.getAccount().getUuid(),
        auth.getAccount().getNumber(),
        auth.getAccount().getPhoneNumberIdentifier(),
        auth.getAccount().getUsernameHash().filter(h -> h.length > 0).orElse(null),
        auth.getAccount().isStorageSupported());
  }

  @DELETE
  @Path("/username_hash")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Delete username hash",
      description = """
          Authenticated endpoint. Deletes previously stored username for the account.
          """
  )
  @ApiResponse(responseCode = "204", description = "Username successfully deleted.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "401", description = "Account authentication check failed.")
  public CompletableFuture<Void> deleteUsernameHash(@Auth final AuthenticatedAccount auth) {
    return accounts.clearUsernameHash(auth.getAccount())
        .thenRun(Util.NOOP);
  }

  @PUT
  @Path("/username_hash/reserve")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Reserve username hash",
      description = """
          Authenticated endpoint. Takes in a list of hashes of potential username hashes, finds one that is not taken,
          and reserves it for the current account.
          """
  )
  @ApiResponse(responseCode = "200", description = "Username hash reserved successfully.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "401", description = "Account authentication check failed.")
  @ApiResponse(responseCode = "409", description = "All username hashes from the list are taken.")
  @ApiResponse(responseCode = "422", description = "Invalid request format.")
  @ApiResponse(responseCode = "429", description = "Ratelimited.")
  public CompletableFuture<ReserveUsernameHashResponse> reserveUsernameHash(
      @Auth final AuthenticatedAccount auth,
      @NotNull @Valid final ReserveUsernameHashRequest usernameRequest) throws RateLimitExceededException {

    rateLimiters.getUsernameReserveLimiter().validate(auth.getAccount().getUuid());

    for (final byte[] hash : usernameRequest.usernameHashes()) {
      if (hash.length != USERNAME_HASH_LENGTH) {
        throw new WebApplicationException(Response.status(422).build());
      }
    }

    return accounts.reserveUsernameHash(auth.getAccount(), usernameRequest.usernameHashes())
        .thenApply(reservation -> new ReserveUsernameHashResponse(reservation.reservedUsernameHash()))
        .exceptionally(throwable -> {
          if (ExceptionUtils.unwrap(throwable) instanceof UsernameHashNotAvailableException) {
            throw new WebApplicationException(Status.CONFLICT);
          }

          throw ExceptionUtils.wrap(throwable);
        });
  }

  @PUT
  @Path("/username_hash/confirm")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Confirm username hash",
      description = """
          Authenticated endpoint. For a previously reserved username hash, confirm that this username hash is now taken
          by this account.
          """
  )
  @ApiResponse(responseCode = "200", description = "Username hash confirmed successfully.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "401", description = "Account authentication check failed.")
  @ApiResponse(responseCode = "409", description = "Given username hash doesn't match the reserved one or no reservation found.")
  @ApiResponse(responseCode = "410", description = "Username hash not available (username can't be used).")
  @ApiResponse(responseCode = "422", description = "Invalid request format.")
  @ApiResponse(responseCode = "429", description = "Ratelimited.")
  public CompletableFuture<UsernameHashResponse> confirmUsernameHash(
      @Auth final AuthenticatedAccount auth,
      @NotNull @Valid final ConfirmUsernameHashRequest confirmRequest) {

    try {
      usernameHashZkProofVerifier.verifyProof(confirmRequest.zkProof(), confirmRequest.usernameHash());
    } catch (final BaseUsernameException e) {
      throw new WebApplicationException(Response.status(422).build());
    }

    return rateLimiters.getUsernameSetLimiter().validateAsync(auth.getAccount().getUuid())
        .thenCompose(ignored -> accounts.confirmReservedUsernameHash(
            auth.getAccount(),
            confirmRequest.usernameHash(),
            confirmRequest.encryptedUsername()))
        .thenApply(updatedAccount -> new UsernameHashResponse(updatedAccount.getUsernameHash()
            .orElseThrow(() -> new IllegalStateException("Could not get username after setting")),
            updatedAccount.getUsernameLinkHandle()))
        .exceptionally(throwable -> {
          if (ExceptionUtils.unwrap(throwable) instanceof UsernameReservationNotFoundException) {
            throw new WebApplicationException(Status.CONFLICT);
          }

          if (ExceptionUtils.unwrap(throwable) instanceof UsernameHashNotAvailableException) {
            throw new WebApplicationException(Status.GONE);
          }

          throw ExceptionUtils.wrap(throwable);
        })
        .toCompletableFuture();
  }

  @GET
  @Path("/password_sign")
  @Produces(MediaType.APPLICATION_JSON)
  @RateLimitedByIp(RateLimiters.For.USERNAME_LOOKUP)
  @Operation(
      summary = "Lookup username hash",
      description = """
          Forced unauthenticated endpoint. For the given username hash, look up a user ID.
          """
  )
  @ApiResponse(responseCode = "200", description = "Account found for the given username.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "400", description = "Request must not be authenticated.")
  @ApiResponse(responseCode = "404", description = "Account not fount for the given username.")
  public VerificationSessionResponse getPasswordSignKey(
      @Auth final AuthenticatedAccount auth) throws RateLimitExceededException {
    rateLimiters.forDescriptor(RateLimiters.For.GET_PASSWORD_SIGN_KEY_OPERATION).validate(auth.getAccount().getUuid());
    String accountName = auth.getAccount().getNumber();
    if (accountName == null) {
      logger.error("getPasswordSignKey, can not get account name");
      throw new ServerErrorException("could not get account name", Response.Status.BAD_REQUEST);
    }
    final RegistrationServiceSession registrationServiceSession;
    try {
      byte[] sessionId = UUIDUtil.toBytes(UUID.randomUUID());
      registrationServiceSession = new RegistrationServiceSession(sessionId,
          accountName, true, null, null, null, Instant.now().getEpochSecond() + 600);
    } catch (final CancellationException e) {

      throw new ServerErrorException("registration service unavailable", Response.Status.SERVICE_UNAVAILABLE);
    } catch (final CompletionException e) {

      if (ExceptionUtils.unwrap(e) instanceof RateLimitExceededException re) {
        RateLimiter.adaptLegacyException(() -> {
          throw re;
        });
      }

      throw new ServerErrorException(Response.Status.INTERNAL_SERVER_ERROR, e);
    }

    final KeyPair generate = RSAUtils.generate();
    final String publicKey = RSAUtils.keyToBase64(generate.getPublic());
    final String privateKey = RSAUtils.keyToBase64(generate.getPrivate());

//    logger.info("generate key for session : " + registrationServiceSession.encodedSessionId());
//    logger.info("generate private key = " + privateKey);
//    logger.info("generate public key = " + publicKey);
    final Clock clock = Clock.systemUTC();
    VerificationSession verificationSession = new VerificationSession(null, new ArrayList<>(),
        Collections.emptyList(), false,
        clock.millis(), clock.millis(), registrationServiceSession.expiration(),
        accountName,
        publicKey,
        privateKey);

    storeVerificationSession(registrationServiceSession, verificationSession);

    return new VerificationSessionResponse(registrationServiceSession.encodedSessionId(),
        null,
        null,
        null,
        verificationSession.allowedToRequestCode(), verificationSession.requestedInformation(),
        true,
        verificationSession.publicKey());
  }


  @PUT
  @Path("/password")
  @Produces(MediaType.APPLICATION_JSON)
  @RateLimitedByIp(RateLimiters.For.USERNAME_LOOKUP)
  @Operation(
      summary = "Lookup username hash",
      description = """
          Forced unauthenticated endpoint. For the given username hash, look up a user ID.
          """
  )
  @ApiResponse(responseCode = "200", description = "Account found for the given username.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "400", description = "Request must not be authenticated.")
  @ApiResponse(responseCode = "404", description = "Account not fount for the given username.")
  public void changePassword(
      @Auth final AuthenticatedAccount auth, @NotNull @Valid ChangePasswordRequest req)
      throws RateLimitExceededException {
    final Account account = auth.getAccount();
    rateLimiters.forDescriptor(RateLimiters.For.CHANGE_PASSWORD_OPERATION).validate(account.getUuid());
    String accountName = account.getNumber();
    if (accountName == null) {
      logger.error("getPasswordSignKey, can not get account name");
      throw new ServerErrorException("could not get account name", Response.Status.BAD_REQUEST);
    }

    String sessionId = req.sessionId();
    if (sessionId == null) {
      throw new BadRequestException("sessionId is null");
    }
    String oldPwd = req.oldPwd();
    if (oldPwd == null) {
      throw new BadRequestException("oldPwd is null");
    }
    String newPwd = req.newPwd();
    if (newPwd == null) {
      throw new BadRequestException("newPwd is null");
    }

    final VerificationSession verificationSession = retrieveVerificationSession(sessionId);

    String newPwdDecrypted = "";
    try {
      newPwdDecrypted = RSAUtils.decrypt(newPwd, verificationSession.privateKey());
    } catch (GeneralSecurityException e) {
      logger.error("decrypt newPwd error when change password", e);
      throw new BadRequestException("decrypt error new pwd");
    }
    if (newPwdDecrypted.isEmpty()) {
      throw new BadRequestException("decrypt newPwd error, null pwd");
    }

    String oldPwdDecrypted = "";
    try {
      oldPwdDecrypted = RSAUtils.decrypt(oldPwd, verificationSession.privateKey());
    } catch (GeneralSecurityException e) {
      logger.error("decrypt error when change password", e);
      throw new BadRequestException("decrypt error old pwd");
    }
    if (oldPwdDecrypted.isEmpty()) {
      throw new BadRequestException("decrypt error, null pwd");
    }
//    logger.info("register n" + "umber="+number+", after pwd = " + pwd);
    if (!Objects.equals(oldPwdDecrypted, account.getPwd())) {
      logger.error("修改密码失败：" + account.getNumber() + ", 密码错误");
      throw new NotAuthorizedException("wrong password", Response.Status.UNAUTHORIZED);
    }

    account.setPwd(newPwdDecrypted);
    accounts.updateAsync(account,(acc -> {}));
  }

  private VerificationSession retrieveVerificationSession(String encodedSessionId) {

    return verificationSessionManager.findForId(encodedSessionId)
        .orTimeout(5, TimeUnit.SECONDS)
        .join().orElseThrow(NotFoundException::new);
  }


  private void storeVerificationSession(final RegistrationServiceSession registrationServiceSession,
      final VerificationSession verificationSession) {
    verificationSessionManager.insert(registrationServiceSession.encodedSessionId(), verificationSession)
        .orTimeout(DYNAMODB_TIMEOUT.toSeconds(), TimeUnit.SECONDS)
        .join();
  }

  @GET
  @Path("/account_id/{aci}")
  @Produces(MediaType.APPLICATION_JSON)
  @RateLimitedByIp(RateLimiters.For.USERNAME_LOOKUP)
  @Operation(
      summary = "Lookup username hash",
      description = """
          Forced unauthenticated endpoint. For the given username hash, look up a user ID.
          """
  )
  @ApiResponse(responseCode = "200", description = "Account found for the given username.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "400", description = "Request must not be authenticated.")
  @ApiResponse(responseCode = "404", description = "Account not fount for the given username.")
  public AccountIdResponse lookupAccountId(
      @Auth final AuthenticatedAccount auth,
      @PathParam("aci") final String aci) throws RateLimitExceededException {

    rateLimiters.forDescriptor(RateLimiters.For.LOOKUP_ACCOUNT_ID_OPERATION).validate(auth.getAccount().getUuid());

    if (aci == null || aci.isEmpty()) {
      throw new WebApplicationException(Response.status(422).build());
    }

    return accounts
        .getByAccountIdentifier(UUID.fromString(aci))
        .map(Account::getNumber)
        .map(AccountIdResponse::new)
        .orElseThrow(() -> new WebApplicationException(Status.NOT_FOUND));
  }

  @GET
  @Path("/aci/{accountId}")
  @Produces(MediaType.APPLICATION_JSON)
  @RateLimitedByIp(RateLimiters.For.USERNAME_LOOKUP)
  @Operation(
      summary = "Lookup username hash",
      description = """
          Forced unauthenticated endpoint. For the given username hash, look up a user ID.
          """
  )
  @ApiResponse(responseCode = "200", description = "Account found for the given username.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "400", description = "Request must not be authenticated.")
  @ApiResponse(responseCode = "404", description = "Account not fount for the given username.")
  public AccountIdentifierResponse lookupACI(
      @Auth final AuthenticatedAccount auth,
      @PathParam("accountId") final String accountId) throws RateLimitExceededException {

    rateLimiters.forDescriptor(RateLimiters.For.LOOKUP_ACI_OPERATION).validate(auth.getAccount().getUuid());

    if (accountId == null || accountId.isEmpty()) {
      throw new WebApplicationException(Response.status(422).build());
    }

    return accounts
        .getByE164(accountId)
        .map(Account::getUuid)
        .map(AciServiceIdentifier::new)
        .map(AccountIdentifierResponse::new)
        .orElseThrow(() -> new WebApplicationException(Status.NOT_FOUND));
  }

  @GET
  @Path("/totp")
  @Produces(MediaType.APPLICATION_JSON)
  @RateLimitedByIp(RateLimiters.For.USERNAME_LOOKUP)
  @Operation(
      summary = "Lookup username hash",
      description = """
          Forced unauthenticated endpoint. For the given username hash, look up a user ID.
          """
  )
  @ApiResponse(responseCode = "200", description = "Account found for the given username.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "400", description = "Request must not be authenticated.")
  @ApiResponse(responseCode = "404", description = "Account not fount for the given username.")
  public AccountTotpGenResponse genTOTP(@Auth final AuthenticatedAccount auth) throws RateLimitExceededException {
    rateLimiters.forDescriptor(RateLimiters.For.GEN_TOTP_OPERATION).validate(auth.getAccount().getUuid());
    byte[] randomBytes = new byte[20];
    new SecureRandom().nextBytes(randomBytes);
    // Encode the random byte array using Base32
    Base32 base32 = new Base32();

    String secretKey =  base32.encodeAsString(randomBytes);
    String issuer = "千里传音";
    String otpAuthURL = String.format("otpauth://totp/%s:%s?secret=%s&issuer=%s", issuer, auth.getAccount().getNumber(), secretKey, issuer);

    //remember secretKey
    accounts.update(auth.getAccount(), a -> a.setTotpSecretKey(secretKey));

    return new AccountTotpGenResponse(otpAuthURL);
  }

  @GET
  @Path("/totp/{otp}")
  @Produces(MediaType.APPLICATION_JSON)
  @RateLimitedByIp(RateLimiters.For.USERNAME_LOOKUP)
  @Operation(
      summary = "Lookup username hash",
      description = """
          Forced unauthenticated endpoint. For the given username hash, look up a user ID.
          """
  )
  @ApiResponse(responseCode = "200", description = "Account found for the given username.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "400", description = "Request must not be authenticated.")
  @ApiResponse(responseCode = "404", description = "Account not fount for the given username.")
  public AccountTotpBindResponse bindTOTP(@Auth final AuthenticatedAccount auth, @PathParam("otp") String userOtp) throws RateLimitExceededException {
    rateLimiters.forDescriptor(RateLimiters.For.GEN_TOTP_OPERATION).validate(auth.getAccount().getUuid());

    String secretKey = auth.getAccount().getTotpSecretKey();

    try {
      boolean validate = TotpUtil.validate(secretKey,userOtp);
      if(validate){
        //验证正确，标记为已绑定，再登录的时候就要验证了
        accounts.update(auth.getAccount(), a -> a.setTotpBind(true));
      }
      return new AccountTotpBindResponse(validate);
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      logger.error("err when validate user otp when bind",e);
      return new AccountTotpBindResponse(false);
    }
  }

  @GET
  @Path("/username_hash/{usernameHash}")
  @Produces(MediaType.APPLICATION_JSON)
  @RateLimitedByIp(RateLimiters.For.USERNAME_LOOKUP)
  @Operation(
      summary = "Lookup username hash",
      description = """
          Forced unauthenticated endpoint. For the given username hash, look up a user ID.
          """
  )
  @ApiResponse(responseCode = "200", description = "Account found for the given username.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "400", description = "Request must not be authenticated.")
  @ApiResponse(responseCode = "404", description = "Account not found for the given username.")
  public CompletableFuture<AccountIdentifierResponse> lookupUsernameHash(
      @Auth final Optional<AuthenticatedAccount> maybeAuthenticatedAccount,
      @PathParam("usernameHash") final String usernameHash) {

    requireNotAuthenticated(maybeAuthenticatedAccount);
    final byte[] hash;
    try {
      hash = Base64.getUrlDecoder().decode(usernameHash);
    } catch (IllegalArgumentException | AssertionError e) {
      throw new WebApplicationException(Response.status(422).build());
    }

    if (hash.length != USERNAME_HASH_LENGTH) {
      throw new WebApplicationException(Response.status(422).build());
    }

    return accounts.getByUsernameHash(hash).thenApply(maybeAccount -> maybeAccount.map(Account::getUuid)
        .map(AciServiceIdentifier::new)
        .map(AccountIdentifierResponse::new)
        .orElseThrow(() -> new WebApplicationException(Status.NOT_FOUND)));
  }

  @PUT
  @Path("/username_link")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Set username link",
      description = """
          Authenticated endpoint. For the given encrypted username generates a username link handle.
          Username link handle could be used to lookup the encrypted username.
          An account can only have one username link at a time. Calling this endpoint will reset previously stored
          encrypted username and deactivate previous link handle.
          """
  )
  @ApiResponse(responseCode = "200", description = "Username Link updated successfully.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "401", description = "Account authentication check failed.")
  @ApiResponse(responseCode = "409", description = "Username is not set for the account.")
  @ApiResponse(responseCode = "422", description = "Invalid request format.")
  @ApiResponse(responseCode = "429", description = "Ratelimited.")
  public UsernameLinkHandle updateUsernameLink(
      @Auth final AuthenticatedAccount auth,
      @NotNull @Valid final EncryptedUsername encryptedUsername) throws RateLimitExceededException {
    // check ratelimiter for username link operations
    rateLimiters.forDescriptor(RateLimiters.For.USERNAME_LINK_OPERATION).validate(auth.getAccount().getUuid());

    // check if username hash is set for the account
    if (auth.getAccount().getUsernameHash().isEmpty()) {
      throw new WebApplicationException(Status.CONFLICT);
    }

    final UUID usernameLinkHandle = UUID.randomUUID();
    updateUsernameLink(auth.getAccount(), usernameLinkHandle, encryptedUsername.usernameLinkEncryptedValue());
    return new UsernameLinkHandle(usernameLinkHandle);
  }

  @DELETE
  @Path("/username_link")
  @Operation(
      summary = "Delete username link",
      description = """
          Authenticated endpoint. Deletes username link for the given account: previously store encrypted username is deleted
          and username link handle is deactivated.
          """
  )
  @ApiResponse(responseCode = "204", description = "Username Link successfully deleted.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "401", description = "Account authentication check failed.")
  @ApiResponse(responseCode = "429", description = "Ratelimited.")
  public void deleteUsernameLink(@Auth final AuthenticatedAccount auth) throws RateLimitExceededException {
    // check ratelimiter for username link operations
    rateLimiters.forDescriptor(RateLimiters.For.USERNAME_LINK_OPERATION).validate(auth.getAccount().getUuid());
    clearUsernameLink(auth.getAccount());
  }

  @GET
  @Path("/username_link/{uuid}")
  @Produces(MediaType.APPLICATION_JSON)
  @RateLimitedByIp(RateLimiters.For.USERNAME_LINK_LOOKUP_PER_IP)
  @Operation(
      summary = "Lookup username link",
      description = """
          Enforced unauthenticated endpoint. For the given username link handle, looks up the database for an associated encrypted username.
          If found, encrypted username is returned, otherwise responds with 404 Not Found.
          """
  )
  @ApiResponse(responseCode = "200", description = "Username link with the given handle was found.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "400", description = "Request must not be authenticated.")
  @ApiResponse(responseCode = "404", description = "Username link was not found for the given handle.")
  @ApiResponse(responseCode = "422", description = "Invalid request format.")
  @ApiResponse(responseCode = "429", description = "Ratelimited.")
  public CompletableFuture<EncryptedUsername> lookupUsernameLink(
      @Auth final Optional<AuthenticatedAccount> maybeAuthenticatedAccount,
      @PathParam("uuid") final UUID usernameLinkHandle) {

    requireNotAuthenticated(maybeAuthenticatedAccount);

    return accounts.getByUsernameLinkHandle(usernameLinkHandle)
        .thenApply(maybeAccount -> maybeAccount.flatMap(Account::getEncryptedUsername)
            .map(EncryptedUsername::new)
            .orElseThrow(NotFoundException::new));
  }

  @Operation(
      summary = "Check whether an account exists",
      description = """
          Enforced unauthenticated endpoint. Checks whether an account with a given identifier exists.
          """
  )
  @ApiResponse(responseCode = "200", description = "An account with the given identifier was found.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "400", description = "Request must not be authenticated.")
  @ApiResponse(responseCode = "404", description = "An account was not found for the given identifier.")
  @ApiResponse(responseCode = "422", description = "Invalid request format.")
  @ApiResponse(responseCode = "429", description = "Rate-limited.")
  @HEAD
  @Path("/account/{identifier}")
  @RateLimitedByIp(RateLimiters.For.CHECK_ACCOUNT_EXISTENCE)
  public Response accountExists(
      @Auth final Optional<AuthenticatedAccount> authenticatedAccount,

      @Parameter(description = "An ACI or PNI account identifier to check")
      @PathParam("identifier") final ServiceIdentifier accountIdentifier) {

    // Disallow clients from making authenticated requests to this endpoint
    requireNotAuthenticated(authenticatedAccount);

    final Optional<Account> maybeAccount = accounts.getByServiceIdentifier(accountIdentifier);

    return Response.status(maybeAccount.map(ignored -> Status.OK).orElse(Status.NOT_FOUND)).build();
  }

  @DELETE
  @Path("/me")
  public CompletableFuture<Void> deleteAccount(@Auth DisabledPermittedAuthenticatedAccount auth)
      throws InterruptedException {
    return accounts.delete(auth.getAccount(), AccountsManager.DeletionReason.USER_REQUEST);
  }

  private void clearUsernameLink(final Account account) {
    updateUsernameLink(account, null, null);
  }

  private void updateUsernameLink(
      final Account account,
      @Nullable final UUID usernameLinkHandle,
      @Nullable final byte[] encryptedUsername) {
    if ((encryptedUsername == null) ^ (usernameLinkHandle == null)) {
      throw new IllegalStateException("Both or neither arguments must be null");
    }
    accounts.update(account, a -> a.setUsernameLinkDetails(usernameLinkHandle, encryptedUsername));
  }

  private void requireNotAuthenticated(final Optional<AuthenticatedAccount> authenticatedAccount) {
    if (authenticatedAccount.isPresent()) {
      throw new BadRequestException("Operation requires unauthenticated access");
    }
  }
}
