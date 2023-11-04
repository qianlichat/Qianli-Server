/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;

public record ChangePasswordRequest(
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = "Session id hold rsa key")
    String sessionId,

    @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = "old password for account")
    String oldPwd,

    @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = "new password for account")
    String newPwd){

  @JsonCreator
  public ChangePasswordRequest(@JsonProperty("sessionId") String sessionId,
      @JsonProperty("oldPwd") String oldPwd,
      @JsonProperty("newPwd") String newPwd) {
    this.sessionId = sessionId;
    this.newPwd = newPwd;
    this.oldPwd = oldPwd;
  }
}
