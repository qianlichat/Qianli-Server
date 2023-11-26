package org.whispersystems.textsecuregcm.util;

import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.controllers.AccountController;

public class TotpUtil {

//  private static final Logger logger = LoggerFactory.getLogger(TotpUtil.class);

  public static boolean validate(String secretKey, String userOtp)
      throws NoSuchAlgorithmException, InvalidKeyException {
    // Decode the secret key from Base32
    Base32 base32 = new Base32();
    byte[] keyBytes = base32.decode(secretKey);

    // Generate a timestamp in 30-second intervals
    long timestamp = Instant.now().getEpochSecond() / 30;

    String t = timeStamp2Date(timestamp,null);
//    logger.info("check otp, time= :" + t + ", user otp:" + userOtp + ", secretKey=" + secretKey);

    // Convert the timestamp to bytes
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putLong(timestamp);
    byte[] timestampBytes = buffer.array();

    // Initialize HMAC-SHA1 with the secret key
    Mac hmacSha1 = Mac.getInstance("HmacSHA1");
    SecretKeySpec secretKeySpec = new SecretKeySpec(keyBytes, "RAW");
    hmacSha1.init(secretKeySpec);

    // Calculate the HMAC-SHA1 hash of the timestamp
    byte[] hash = hmacSha1.doFinal(timestampBytes);

    // Extract the 4 least significant bits of the last byte to determine the starting offset
    int offset = hash[hash.length - 1] & 0xF;

    // Extract a 4-byte dynamic binary code from the hash starting at the offset
    byte[] dynamicCodeBytes = Arrays.copyOfRange(hash, offset, offset + 4);

    // Convert the dynamic code to an integer
    int dynamicCode = ByteBuffer.wrap(dynamicCodeBytes).getInt();

    // Remove the most significant bit (sign bit) to make it non-negative
    dynamicCode &= 0x7FFFFFFF;

    // Calculate the one-time password as a 6-digit number
    int otp = dynamicCode % 1000000;

    // Convert the OTP to a string with leading zeros
    String formattedOTP = String.format("%06d", otp);

//    logger.info("check otp, calculated :" + formattedOTP + ", user otp:" + userOtp + ", secretKey=" + secretKey);

    // Compare the user-input OTP with the calculated OTP
    return formattedOTP.equals(userOtp);
  }

  public static String timeStamp2Date(long seconds, String format) {
    if (format == null || format.isEmpty()) {
      format = "yyyy-MM-dd HH:mm:ss";
    }
    SimpleDateFormat sdf = new SimpleDateFormat(format);
    return sdf.format(new Date(seconds * 1000));
  }
}
