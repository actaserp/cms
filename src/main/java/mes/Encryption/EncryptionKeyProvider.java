package mes.Encryption;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;

@Slf4j
public class EncryptionKeyProvider {

    private static final String WINDOWS_KEY_FILE_PATH = "C:/secret/aes256.key";
    private static final String LINUX_KEY_FILE_PATH = "/opt/secret/aes256.key";

    private static volatile byte[] cachedKey;

    public static String getKeyFilepath(){
        String os = System.getProperty("os.name").toLowerCase();
        return os.contains("win") ? WINDOWS_KEY_FILE_PATH : LINUX_KEY_FILE_PATH;
    }

    public static byte[] getKey() throws IOException {
        if (cachedKey == null) {
            synchronized (EncryptionKeyProvider.class) {
                if (cachedKey == null) {
                    log.info("AES Key 파일 처음 로딩합니다...");
                    String keyFilePath = getKeyFilepath();
                    String encodedKey = new String(Files.readAllBytes(Paths.get(keyFilePath)), StandardCharsets.UTF_8)
                            .replaceAll("\\s+", "");
                    byte[] decodedKey = Base64.getDecoder().decode(encodedKey);
                    if (decodedKey.length != 32) {
                        throw new IllegalArgumentException("AES-256 키는 정확히 32바이트여야 합니다.");
                    }
                    cachedKey = decodedKey;
                }
            }
        }
        return cachedKey;
    }
}
