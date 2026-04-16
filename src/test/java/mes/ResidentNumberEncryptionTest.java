package mes;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;

import static org.junit.Assert.assertEquals;

public class ResidentNumberEncryptionTest {

    private byte[] getKeyFromFile() throws Exception {
        String os = System.getProperty("os.name").toLowerCase();
        String path = os.contains("win") ? "C:/secret/aes256.key" : "/opt/secret/aes256.key";

        String encodedKey = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8)
                .replaceAll("\\s+", ""); // 줄바꿈, 공백 제거

        System.out.println("읽은 키 문자열: " + encodedKey);

        byte[] decodedKey = Base64.getDecoder().decode(encodedKey);

        if (decodedKey.length != 32)
            throw new IllegalArgumentException("AES-256 키는 정확히 32바이트여야 합니다.");
        return decodedKey;
    }

    private String encrypt(String plainText, byte[] keyBytes) throws Exception {
        Cipher cipher = Cipher.getInstance("AES");
        SecretKeySpec keySpec = new SecretKeySpec(keyBytes, "AES");
        cipher.init(Cipher.ENCRYPT_MODE, keySpec);
        return Base64.getEncoder().encodeToString(cipher.doFinal(plainText.getBytes("UTF-8")));
    }

    private String decrypt(String encryptedText, byte[] keyBytes) throws Exception {
        Cipher cipher = Cipher.getInstance("AES");
        SecretKeySpec keySpec = new SecretKeySpec(keyBytes, "AES");
        cipher.init(Cipher.DECRYPT_MODE, keySpec);
        return new String(cipher.doFinal(Base64.getDecoder().decode(encryptedText)), "UTF-8");
    }

    @Test
    public void testResidentNumberEncryptionAndDecryption() throws Exception {

        byte[] keyBytes = getKeyFromFile();

        String original = "8890";
        String encrypted = encrypt(original, keyBytes);
        String decrypted = decrypt(encrypted, keyBytes);

        System.out.println("Encrypted: " + encrypted);
        System.out.println("Decrypted: " + decrypted);

        assertEquals(original, decrypted);
    }
}
