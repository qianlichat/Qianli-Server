/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.math.BigDecimal;
import java.time.Clock;
import java.util.Map;
import java.util.Set;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.signal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAuthenticatedAccount;
import org.whispersystems.textsecuregcm.configuration.SubscriptionConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionLevelConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionPriceConfiguration;
import org.whispersystems.textsecuregcm.controllers.SubscriptionController.GetLevelsResponse;
import org.whispersystems.textsecuregcm.storage.IssuedReceiptsManager;
import org.whispersystems.textsecuregcm.storage.SubscriptionManager;
import org.whispersystems.textsecuregcm.stripe.StripeManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;

@ExtendWith(DropwizardExtensionsSupport.class)
class SubscriptionControllerTest {

  private static final Clock CLOCK = mock(Clock.class);
  private static final SubscriptionConfiguration SUBSCRIPTION_CONFIG = mock(SubscriptionConfiguration.class);
  private static final SubscriptionManager SUBSCRIPTION_MANAGER = mock(SubscriptionManager.class);
  private static final StripeManager STRIPE_MANAGER = mock(StripeManager.class);
  private static final ServerZkReceiptOperations ZK_OPS = mock(ServerZkReceiptOperations.class);
  private static final IssuedReceiptsManager ISSUED_RECEIPTS_MANAGER = mock(IssuedReceiptsManager.class);
  private static final SubscriptionController SUBSCRIPTION_CONTROLLER = new SubscriptionController(
      CLOCK, SUBSCRIPTION_CONFIG, SUBSCRIPTION_MANAGER, STRIPE_MANAGER, ZK_OPS, ISSUED_RECEIPTS_MANAGER);
  private static final ResourceExtension RESOURCE_EXTENSION = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(Set.of(
          AuthenticatedAccount.class, DisabledPermittedAuthenticatedAccount.class)))
      .setMapper(SystemMapper.getMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(SUBSCRIPTION_CONTROLLER)
      .build();

  @AfterEach
  void tearDown() {
    reset(CLOCK, SUBSCRIPTION_CONFIG, SUBSCRIPTION_MANAGER, STRIPE_MANAGER, ZK_OPS, ISSUED_RECEIPTS_MANAGER);
  }

  @Test
  void getLevels() {
    when(SUBSCRIPTION_CONFIG.getLevels()).thenReturn(Map.of(
        1L, new SubscriptionLevelConfiguration("B1", "P1", Map.of("USD", new SubscriptionPriceConfiguration("R1", BigDecimal.valueOf(100)))),
        2L, new SubscriptionLevelConfiguration("B2", "P2", Map.of("USD", new SubscriptionPriceConfiguration("R2", BigDecimal.valueOf(200)))),
        3L, new SubscriptionLevelConfiguration("B3", "P3", Map.of("USD", new SubscriptionPriceConfiguration("R3", BigDecimal.valueOf(300))))
    ));

    GetLevelsResponse response = RESOURCE_EXTENSION.target("/v1/subscription/levels")
        .request()
        .get(GetLevelsResponse.class);

    assertThat(response.getLevels()).containsKeys(1L, 2L, 3L).satisfies(longLevelMap -> {
      assertThat(longLevelMap).extractingByKey(1L).satisfies(level -> {
        assertThat(level.getBadgeId()).isEqualTo("B1");
        assertThat(level.getCurrencies()).containsKeys("USD").extractingByKey("USD").satisfies(price -> {
          assertThat(price).isEqualTo("100");
        });
      });
      assertThat(longLevelMap).extractingByKey(2L).satisfies(level -> {
        assertThat(level.getBadgeId()).isEqualTo("B2");
        assertThat(level.getCurrencies()).containsKeys("USD").extractingByKey("USD").satisfies(price -> {
          assertThat(price).isEqualTo("200");
        });
      });
      assertThat(longLevelMap).extractingByKey(3L).satisfies(level -> {
        assertThat(level.getBadgeId()).isEqualTo("B3");
        assertThat(level.getCurrencies()).containsKeys("USD").extractingByKey("USD").satisfies(price -> {
          assertThat(price).isEqualTo("300");
        });
      });
    });
  }
}
