package org.whispersystems.textsecuregcm.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.controllers.VerificationController;
import javax.crypto.Cipher;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Base64;

public class RSAUtils {
  private static final Logger logger = LoggerFactory.getLogger(RSAUtils.class);

  public static KeyPair generate() {
    try {
      KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
      keyGen.initialize(2048);
      return keyGen.generateKeyPair();
    } catch (NoSuchAlgorithmException e) {
      logger.error("generate failed !",e);
      throw new RuntimeException(e);
    }
  }

  public static String keyToBase64(Key publicKey) {
    return Base64.getEncoder().encodeToString(publicKey.getEncoded());
  }

  public static String encrypt(String plainText, PublicKey publicKey) {
    try{
      Cipher encryptCipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-256AndMGF1Padding");
      encryptCipher.init(Cipher.ENCRYPT_MODE, publicKey);
      return Base64.getEncoder().encodeToString(encryptCipher.doFinal(plainText.getBytes()));
    }catch (Throwable e){
      logger.error("encrypt failed !",e);
    }
    return "";
  }

  public static String decrypt(String base64CipherText, PrivateKey privateKey){
    try{
      byte[] cipherText = Base64.getDecoder().decode(base64CipherText);
      Cipher decryptCipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-256AndMGF1Padding");
      decryptCipher.init(Cipher.DECRYPT_MODE, privateKey);
      return new String(decryptCipher.doFinal(cipherText));
    }catch (Throwable e){
      logger.error("decrypt failed !",e);
    }
    return "";
  }
}
