/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

/*
 * This is derived from Coursera's dropwizard datadog reporter.
 * https://github.com/coursera/metrics-datadog
 */

package org.whispersystems.textsecuregcm.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dropwizard.metrics.BaseReporterFactory;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import org.coursera.metrics.datadog.DatadogReporter;
import org.coursera.metrics.datadog.DatadogReporter.Expansion;
import org.coursera.metrics.datadog.DefaultMetricNameFormatterFactory;
import org.coursera.metrics.datadog.DynamicTagsCallbackFactory;
import org.coursera.metrics.datadog.MetricNameFormatterFactory;
import org.coursera.metrics.datadog.transport.UdpTransport;
import org.whispersystems.textsecuregcm.WhisperServerVersion;
import org.whispersystems.textsecuregcm.util.HostnameUtil;

@JsonTypeName("signal-datadog")
public class SignalDatadogReporterFactory extends BaseReporterFactory {

  @JsonProperty
  private List<String> tags = null;

  @Valid
  @JsonProperty
  private DynamicTagsCallbackFactory dynamicTagsCallback = null;

  @JsonProperty
  private String prefix = null;

  @Valid
  @NotNull
  @JsonProperty
  private MetricNameFormatterFactory metricNameFormatter = new DefaultMetricNameFormatterFactory();

  @Valid
  @NotNull
  @JsonProperty("udpTransport")
  private UdpTransportConfig udpTransportConfig;

  private static final EnumSet<Expansion> EXPANSIONS = EnumSet.of(
      Expansion.COUNT,
      Expansion.MIN,
      Expansion.MAX,
      Expansion.MEAN,
      Expansion.MEDIAN,
      Expansion.P75,
      Expansion.P95,
      Expansion.P99,
      Expansion.P999
  );

  public ScheduledReporter build(final MetricRegistry registry) {
    final List<String> tagsWithVersion;

    {
      final String versionTag = "version:" + WhisperServerVersion.getServerVersion();

      if (tags != null) {
        tagsWithVersion = new ArrayList<>(tags);
        tagsWithVersion.add(versionTag);
      } else {
        tagsWithVersion = List.of(versionTag);
      }
    }

    return DatadogReporter.forRegistry(registry)
        .withTransport(udpTransportConfig.udpTransport())
        .withHost(HostnameUtil.getLocalHostname())
        .withTags(tagsWithVersion)
        .withPrefix(prefix)
        .withExpansions(EXPANSIONS)
        .withMetricNameFormatter(metricNameFormatter.build())
        .withDynamicTagCallback(dynamicTagsCallback != null ? dynamicTagsCallback.build() : null)
        .filter(getFilter())
        .convertDurationsTo(getDurationUnit())
        .convertRatesTo(getRateUnit())
        .build();
  }

  public record UdpTransportConfig(@NotNull String statsdHost, @Min(1) int port) {

    public UdpTransport udpTransport() {
      return new UdpTransport.Builder()
          .withStatsdHost(statsdHost)
          .withPort(port)
          .build();
    }
  }
}
