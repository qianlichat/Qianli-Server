/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.util;

import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberFormat;
import com.google.i18n.phonenumbers.Phonenumber.PhoneNumber;
import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Locale.LanguageRange;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

public class Util {

  private static final Pattern COUNTRY_CODE_PATTERN = Pattern.compile("^\\+([17]|2[07]|3[0123469]|4[013456789]|5[12345678]|6[0123456]|8[1246]|9[0123458]|\\d{3})");

  private static final PhoneNumberUtil PHONE_NUMBER_UTIL = PhoneNumberUtil.getInstance();

  public static final Runnable NOOP = () -> {};

  /**
   * Checks that the given number is a valid, E164-normalized phone number.
   *
   * @param number the number to check
   *
   * @throws ImpossiblePhoneNumberException if the given number is not a valid phone number at all
   * @throws NonNormalizedPhoneNumberException if the given number is a valid phone number, but isn't E164-normalized
   */
  public static void requireNormalizedNumber(final String number) throws ImpossiblePhoneNumberException, NonNormalizedPhoneNumberException {
    if (!PHONE_NUMBER_UTIL.isPossibleNumber(number, null)) {
      throw new ImpossiblePhoneNumberException();
    }

    try {
      final PhoneNumber inputNumber = PHONE_NUMBER_UTIL.parse(number, null);

      // For normalization, we want to format from a version parsed with the country code removed.
      // This handles some cases of "possible", but non-normalized input numbers with a doubled country code, that is
      // with the format "+{country code} {country code} {national number}"
      final int countryCode = inputNumber.getCountryCode();
      final String region = PHONE_NUMBER_UTIL.getRegionCodeForCountryCode(countryCode);

      final PhoneNumber normalizedNumber = switch (region) {
        // the country code has no associated region. Be lenient (and simple) and accept the input number
        case "ZZ", "001" -> inputNumber;
        default -> {
          final String maybeLeadingZero =
              inputNumber.hasItalianLeadingZero() && inputNumber.isItalianLeadingZero() ? "0" : "";
          yield PHONE_NUMBER_UTIL.parse(
              maybeLeadingZero + inputNumber.getNationalNumber(), region);
        }
      };

      final String normalizedE164 = PHONE_NUMBER_UTIL.format(normalizedNumber,
          PhoneNumberFormat.E164);

      if (!number.equals(normalizedE164)) {
        throw new NonNormalizedPhoneNumberException(number, normalizedE164);
      }
    } catch (final NumberParseException e) {
      throw new ImpossiblePhoneNumberException(e);
    }
  }

  public static String getCountryCode(String number) {
    return "0";
//    Matcher matcher = COUNTRY_CODE_PATTERN.matcher(number);
//
//    if (matcher.find()) return matcher.group(1);
//    else                return "0";
  }

  public static String getRegion(final String number) {
    return "ZZ";
//    try {
//      final PhoneNumber phoneNumber = PHONE_NUMBER_UTIL.parse(number, null);
//      return StringUtils.defaultIfBlank(PHONE_NUMBER_UTIL.getRegionCodeForNumber(phoneNumber), "ZZ");
//    } catch (final NumberParseException e) {
//      return "ZZ";
//    }
  }

  public static String getNumberPrefix(String number) {
    String countryCode  = getCountryCode(number);
    int    remaining    = number.length() - (1 + countryCode.length());
    int    prefixLength = Math.min(4, remaining);

    return number.substring(0, 1 + countryCode.length() + prefixLength);
  }

  public static byte[] truncate(byte[] element, int length) {
    byte[] result = new byte[length];
    System.arraycopy(element, 0, result, 0, result.length);

    return result;
  }

  public static void sleep(long i) {
    try {
      Thread.sleep(i);
    } catch (InterruptedException ie) {}
  }

  public static long todayInMillis() {
    return todayInMillis(Clock.systemUTC());
  }

  public static long todayInMillis(Clock clock) {
    return TimeUnit.DAYS.toMillis(TimeUnit.MILLISECONDS.toDays(clock.millis()));
  }

  public static long todayInMillisGivenOffsetFromNow(Clock clock, Duration offset) {
    final long ms = offset.toMillis() + clock.millis();
    return TimeUnit.DAYS.toMillis(TimeUnit.MILLISECONDS.toDays(ms));
  }

  public static Optional<String> findBestLocale(List<LanguageRange> priorityList, Collection<String> supportedLocales) {
    return Optional.ofNullable(Locale.lookupTag(priorityList, supportedLocales));
  }

  /**
   * Map ints to non-negative ints.
   * <br>
   * Unlike Math.abs this method handles Integer.MIN_VALUE correctly.
   *
   * @param n any int value
   * @return an int value guaranteed to be non-negative
   */
  public static int ensureNonNegativeInt(int n) {
    return n == Integer.MIN_VALUE ? 0 : Math.abs(n);
  }

  /**
   * Map longs to non-negative longs.
   * <br>
   * Unlike Math.abs this method handles Long.MIN_VALUE correctly.
   *
   * @param n any long value
   * @return a long value guaranteed to be non-negative
   */
  public static long ensureNonNegativeLong(long n) {
    return n == Long.MIN_VALUE ? 0 : Math.abs(n);
  }

}
