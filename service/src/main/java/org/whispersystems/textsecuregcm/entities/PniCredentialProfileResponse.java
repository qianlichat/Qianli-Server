/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.signal.zkgroup.profiles.PniCredentialResponse;
import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;

public class PniCredentialProfileResponse extends CredentialProfileResponse {

  @JsonProperty
  @JsonSerialize(using = PniCredentialResponseAdapter.Serializing.class)
  @JsonDeserialize(using = PniCredentialResponseAdapter.Deserializing.class)
  private PniCredentialResponse pniCredential;

  public PniCredentialProfileResponse() {
  }

  public PniCredentialProfileResponse(final VersionedProfileResponse versionedProfileResponse,
      final PniCredentialResponse pniCredential) {

    super(versionedProfileResponse);
    this.pniCredential = pniCredential;
  }

  public PniCredentialResponse getPniCredential() {
    return pniCredential;
  }
}