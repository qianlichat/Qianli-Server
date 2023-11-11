/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.security.KeyPair;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.ServerErrorException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.captcha.RegistrationCaptchaManager;
import org.whispersystems.textsecuregcm.entities.CreateVerificationSessionRequest;
import org.whispersystems.textsecuregcm.entities.RegistrationServiceSession;
import org.whispersystems.textsecuregcm.entities.SubmitVerificationCodeRequest;
import org.whispersystems.textsecuregcm.entities.UpdateVerificationSessionRequest;
import org.whispersystems.textsecuregcm.entities.VerificationCodeRequest;
import org.whispersystems.textsecuregcm.entities.VerificationSessionResponse;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.registration.RegistrationServiceClient;
import org.whispersystems.textsecuregcm.registration.VerificationSession;
import org.whispersystems.textsecuregcm.spam.Extract;
import org.whispersystems.textsecuregcm.spam.FilterSpam;
import org.whispersystems.textsecuregcm.spam.ScoreThreshold;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.storage.VerificationSessionManager;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.RSAUtils;

@Path("/v1/verification")
@io.swagger.v3.oas.annotations.tags.Tag(name = "Verification")
public class VerificationController {

  private static final Logger logger = LoggerFactory.getLogger(VerificationController.class);
  private static final Duration REGISTRATION_RPC_TIMEOUT = Duration.ofSeconds(15);
  public static final Duration DYNAMODB_TIMEOUT = Duration.ofSeconds(5);

  private final RegistrationServiceClient registrationServiceClient;
  private final VerificationSessionManager verificationSessionManager;
  private final RateLimiters rateLimiters;
  private final AccountsManager accountsManager;

  private final Clock clock;

  public VerificationController(final RegistrationServiceClient registrationServiceClient,
      final VerificationSessionManager verificationSessionManager,
      final PushNotificationManager pushNotificationManager,
      final RegistrationCaptchaManager registrationCaptchaManager,
      final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager,
      final RateLimiters rateLimiters,
      final AccountsManager accountsManager,
      final Clock clock) {
    this.registrationServiceClient = registrationServiceClient;
    this.verificationSessionManager = verificationSessionManager;
//    this.pushNotificationManager = pushNotificationManager;
//    this.registrationCaptchaManager = registrationCaptchaManager;
    this.rateLimiters = rateLimiters;
    this.accountsManager = accountsManager;
    this.clock = clock;
  }

  @POST
  @Path("/session")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public VerificationSessionResponse createSession(@NotNull @Valid CreateVerificationSessionRequest request)
      throws RateLimitExceededException {
    final String an = request.getNumber();
    if (an == null) {
      throw new ServerErrorException("could not get account name", Response.Status.BAD_REQUEST);
    }

    RateLimiter.adaptLegacyException(() -> rateLimiters.getRegistrationLimiter().validate(an));

    String accountName = an;
    if (accountName.startsWith("@")) {
      accountName = accountName.substring(1);
    }
    if (!accountName.matches("[a-z0-9]+")) {
      throw new ServerErrorException("account name is invalidate, only lowercase alphabet and numbers are allowed",
          Response.Status.BAD_REQUEST);
    }

    final RegistrationServiceSession registrationServiceSession;
    boolean accountExists = accountsManager.getByE164(accountName).isPresent();
    try {
      registrationServiceSession = registrationServiceClient.createRegistrationSession(accountName,
          accountExists,
          REGISTRATION_RPC_TIMEOUT).join();
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

    VerificationSession verificationSession = new VerificationSession(null, new ArrayList<>(),
        Collections.emptyList(), false,
        clock.millis(), clock.millis(), registrationServiceSession.expiration(),
        accountName,
        publicKey,
        privateKey);

    storeVerificationSession(registrationServiceSession, verificationSession);

    return buildResponse(registrationServiceSession.encodedSessionId(), verificationSession, accountExists);
  }

  @FilterSpam
  @PATCH
  @Path("/session/{sessionId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public VerificationSessionResponse updateSession(@PathParam("sessionId") final String encodedSessionId,
      @HeaderParam(com.google.common.net.HttpHeaders.X_FORWARDED_FOR) String forwardedFor,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent,
      @NotNull @Valid final UpdateVerificationSessionRequest updateVerificationSessionRequest,
      @NotNull @Extract final ScoreThreshold captchaScoreThreshold) {
    throw new ClientErrorException(Response.status(Response.Status.FORBIDDEN)
        .entity("No need this op")
        .build());
  }

  private void storeVerificationSession(final RegistrationServiceSession registrationServiceSession,
      final VerificationSession verificationSession) {
    verificationSessionManager.insert(registrationServiceSession.encodedSessionId(), verificationSession)
        .orTimeout(DYNAMODB_TIMEOUT.toSeconds(), TimeUnit.SECONDS)
        .join();
  }

  @GET
  @Path("/session/{sessionId}")
  @Produces(MediaType.APPLICATION_JSON)
  public VerificationSessionResponse getSession(@PathParam("sessionId") final String encodedSessionId) {
    throw new ServerErrorException("no need this op anymore", Response.Status.SERVICE_UNAVAILABLE);
  }

  @POST
  @Path("/session/{sessionId}/code")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public VerificationSessionResponse requestVerificationCode(@PathParam("sessionId") final String encodedSessionId,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent,
      @HeaderParam(HttpHeaders.ACCEPT_LANGUAGE) Optional<String> acceptLanguage,
      @NotNull @Valid VerificationCodeRequest verificationCodeRequest) {
    throw new ServerErrorException("no need this op anymore", Response.Status.SERVICE_UNAVAILABLE);
  }

  @PUT
  @Path("/session/{sessionId}/code")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public VerificationSessionResponse verifyCode(@PathParam("sessionId") final String encodedSessionId,
      @HeaderParam(HttpHeaders.USER_AGENT) String userAgent,
      @NotNull @Valid final SubmitVerificationCodeRequest submitVerificationCodeRequest)
    /*throws RateLimitExceededException */ {
    throw new ServerErrorException("no need this op anymore", Response.Status.SERVICE_UNAVAILABLE);
//    logger.info("verifyCode here1");
//    final VerificationSession verificationSession = retrieveVerificationSession(encodedSessionId);
//    logger.info("verifyCode here2");
//    String decryptedHash = RSAUtils.decrypt(submitVerificationCodeRequest.code(), verificationSession.privateKey());
//    final Optional<Account> byE164 = accountsManager.getByE164(verificationSession.accountName());
//    boolean accountExists = byE164.isPresent();
//    if(accountExists){
//      if(!decryptedHash.equals(byE164.get().getPwd())){
//        Metrics.counter(VERIFIED_COUNTER_NAME, Tags.of(
//                UserAgentTagUtil.getPlatformTag(userAgent),
//                Tag.of(COUNTRY_CODE_TAG_NAME, "0"),
//                Tag.of(REGION_CODE_TAG_NAME, "ZZ"),
//                Tag.of(SUCCESS_TAG_NAME, Boolean.toString(false))))
//            .increment();
//        logger.error("登录失败："+verificationSession.accountName() + ", 密码错误");
//        throw new ServerErrorException("wrong password", Response.Status.UNAUTHORIZED);
//      }
//    }
//    //重新创建
//
//
//    Metrics.counter(VERIFIED_COUNTER_NAME, Tags.of(
//            UserAgentTagUtil.getPlatformTag(userAgent),
//            Tag.of(COUNTRY_CODE_TAG_NAME, "0"),
//            Tag.of(REGION_CODE_TAG_NAME, "ZZ"),
//            Tag.of(SUCCESS_TAG_NAME, Boolean.toString(true))))
//        .increment();
//    logger.info("verifyCode here3");
//    return buildResponse(resultSession, verificationSession);
  }

  private VerificationSessionResponse buildResponse(String sessionId, final VerificationSession verificationSession,
      boolean accountExists) {
    return new VerificationSessionResponse(sessionId,
        null,
        null,
        null,
        verificationSession.allowedToRequestCode(), verificationSession.requestedInformation(),
        accountExists,
        verificationSession.publicKey());
  }
}
