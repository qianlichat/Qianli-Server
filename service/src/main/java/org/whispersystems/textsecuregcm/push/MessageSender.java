/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.push;

import static com.codahale.metrics.MetricRegistry.name;
import static org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;

import io.micrometer.core.instrument.Metrics;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.controllers.MessageController;
import org.whispersystems.textsecuregcm.redis.RedisOperation;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;

/**
 * A MessageSender sends Signal messages to destination devices. Messages may be "normal" user-to-user messages,
 * ephemeral ("online") messages like typing indicators, or delivery receipts.
 * <p/>
 * If a client is not actively connected to a Signal server to receive a message as soon as it is sent, the
 * MessageSender will send a push notification to the destination device if possible. Some messages may be designated
 * for "online" delivery only and will not be delivered (and clients will not be notified) if the destination device
 * isn't actively connected to a Signal server.
 *
 * @see ClientPresenceManager
 * @see org.whispersystems.textsecuregcm.storage.MessageAvailabilityListener
 * @see ReceiptSender
 */
public class MessageSender {

  private final ClientPresenceManager clientPresenceManager;
  private final MessagesManager messagesManager;
  private final PushNotificationManager pushNotificationManager;
  private final PushLatencyManager pushLatencyManager;

  private static final String SEND_COUNTER_NAME = name(MessageSender.class, "sendMessage");
  private static final String CHANNEL_TAG_NAME = "channel";
  private static final String EPHEMERAL_TAG_NAME = "ephemeral";
  private static final String CLIENT_ONLINE_TAG_NAME = "clientOnline";
  private static final String URGENT_TAG_NAME = "urgent";
  private static final String STORY_TAG_NAME = "story";
  private static final String SEALED_SENDER_TAG_NAME = "sealedSender";
  private static final String HAS_SPAM_REPORTING_TOKEN_TAG_NAME = "hasSpamReportingToken";
  private static final Logger logger = LoggerFactory.getLogger(MessageSender.class);

  public MessageSender(ClientPresenceManager clientPresenceManager,
      MessagesManager messagesManager,
      PushNotificationManager pushNotificationManager,
      PushLatencyManager pushLatencyManager) {
    this.clientPresenceManager = clientPresenceManager;
    this.messagesManager = messagesManager;
    this.pushNotificationManager = pushNotificationManager;
    this.pushLatencyManager = pushLatencyManager;
  }

  public void sendMessage(final Account account, final Device device, final Envelope message, final boolean online)
      throws NotPushRegisteredException {

    final String channel;

    if (device.getGcmId() != null) {
      channel = "gcm";
    } else if (device.getApnId() != null) {
      channel = "apn";
    } else if (device.getFetchesMessages()) {
      channel = "websocket";
    } else {
      throw new AssertionError();
    }

    logger.info("send message through : " + channel + ", online = " + online);

    final boolean clientPresent;

    if (online) {
      clientPresent = clientPresenceManager.isPresent(account.getUuid(), device.getId());

      if (clientPresent) {
        logger.info("send message insert : " + channel + ", online = " + online);
        messagesManager.insert(account.getUuid(), device.getId(), message.toBuilder().setEphemeral(true).build());
        logger.info("send message inserted : " + channel + ", online = " + online);
      }else{
        logger.info("send message insert  fake online : " + channel + ", online = " + online);
      }
    } else {
      messagesManager.insert(account.getUuid(), device.getId(), message);

      // We check for client presence after inserting the message to take a conservative view of notifications. If the
      // client wasn't present at the time of insertion but is now, they'll retrieve the message. If they were present
      // but disconnected before the message was delivered, we should send a notification.
      clientPresent = clientPresenceManager.isPresent(account.getUuid(), device.getId());

      if (!clientPresent) {
        try {
          pushNotificationManager.sendNewMessageNotification(account, device.getId(), message.getUrgent());

          final boolean useVoip = StringUtils.isNotBlank(device.getVoipApnId());
          RedisOperation.unchecked(() -> pushLatencyManager.recordPushSent(account.getUuid(), device.getId(), useVoip, message.getUrgent()));
        } catch (final NotPushRegisteredException e) {
          if (!device.getFetchesMessages()) {
            throw e;
          }
        }
      }
    }

    Metrics.counter(SEND_COUNTER_NAME,
            CHANNEL_TAG_NAME, channel,
            EPHEMERAL_TAG_NAME, String.valueOf(online),
            CLIENT_ONLINE_TAG_NAME, String.valueOf(clientPresent),
            URGENT_TAG_NAME, String.valueOf(message.getUrgent()),
            STORY_TAG_NAME, String.valueOf(message.getStory()),
            SEALED_SENDER_TAG_NAME, String.valueOf(!message.hasSourceUuid()),
            HAS_SPAM_REPORTING_TOKEN_TAG_NAME, String.valueOf(message.getReportSpamToken() != null && !message.getReportSpamToken().isEmpty()))
        .increment();
  }
}
