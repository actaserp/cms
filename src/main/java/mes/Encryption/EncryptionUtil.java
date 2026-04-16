package mes.Encryption;

import io.micrometer.core.instrument.config.validate.Validated;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class EncryptionUtil {

    //암호화
    public static String encrypt(String plainText, byte[] keyBytes) throws Exception {
        Cipher cipher = Cipher.getInstance("AES");
        SecretKeySpec keySpec = new SecretKeySpec(keyBytes, "AES");
        cipher.init(Cipher.ENCRYPT_MODE, keySpec);
        return Base64.getEncoder().encodeToString(cipher.doFinal(plainText.getBytes("UTF-8")));
    }

    //복호화
    public static String decrypt(String encryptedText, byte[] keyBytes) throws Exception {
        Cipher cipher = Cipher.getInstance("AES");
        SecretKeySpec keySpec = new SecretKeySpec(keyBytes, "AES");
        cipher.init(Cipher.DECRYPT_MODE, keySpec);
        return new String(cipher.doFinal(Base64.getDecoder().decode(encryptedText)), "UTF-8");
    }

    public static String encrypt(String plainText) throws Exception {
        byte[] keyBytes = EncryptionKeyProvider.getKey();

        return encrypt(plainText, keyBytes);
    }

    public static String decrypt(String encryptedText) throws Exception {
        byte[] keyBytes = EncryptionKeyProvider.getKey();
        return decrypt(encryptedText, keyBytes);
    }
}