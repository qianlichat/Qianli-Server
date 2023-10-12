package org.whispersystems.textsecuregcm.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.controllers.VerificationController;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;

public class RSAUtils {

  private static final Logger logger = LoggerFactory.getLogger(RSAUtils.class);

  public static KeyPair generate() {
    try {
      KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
      keyGen.initialize(2048);
      return keyGen.generateKeyPair();
    } catch (NoSuchAlgorithmException e) {
      logger.error("generate failed !", e);
      throw new RuntimeException(e);
    }
  }

  public static String keyToBase64(Key publicKey) {
    return Base64.getEncoder().encodeToString(publicKey.getEncoded());
  }

  public static String encrypt(String plainText, PublicKey publicKey) {
    try {
      Cipher encryptCipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-256AndMGF1Padding");
      encryptCipher.init(Cipher.ENCRYPT_MODE, publicKey);
      return Base64.getEncoder().encodeToString(encryptCipher.doFinal(plainText.getBytes()));
    } catch (Throwable e) {
      logger.error("encrypt failed !", e);
    }
    return "";
  }

  public static String decrypt(String base64CipherText, String base64PrivateKey)
      throws GeneralSecurityException {
      PrivateKey privateKey = generatePrivateKeyFromBase64String(base64PrivateKey);
      byte[] cipherText = Base64.getDecoder().decode(base64CipherText);
      Cipher decryptCipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-256AndMGF1Padding");
      decryptCipher.init(Cipher.DECRYPT_MODE, privateKey);
      return new String(decryptCipher.doFinal(cipherText));
  }

  public static PrivateKey generatePrivateKeyFromBase64String(String base64PrivateKey)
      throws GeneralSecurityException {
      // 解码Base64字符串，得到私钥的字节数组
      byte[] privateKeyBytes = Base64.getDecoder().decode(base64PrivateKey);

      // 使用PKCS#8的KeySpec来构造私钥
      PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(privateKeyBytes);
      KeyFactory keyFactory = KeyFactory.getInstance("RSA");

      return keyFactory.generatePrivate(keySpec);
  }
}
