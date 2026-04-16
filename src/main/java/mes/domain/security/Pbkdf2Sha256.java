package mes.domain.security;


import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import javax.crypto.spec.PBEKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.Random;


import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;

public class Pbkdf2Sha256 {
	 /**
     * Length of salt
     */
    public static final int SALT_BYTE_SIZE = 16;

    /**
     * The length of the generated cipher text (for example: 64 * 4, the length of the cipher text is 64)
     */
    public static final int HASH_BIT_SIZE = 64 * 4;

    /**
     * Number of iterations (the default number of iterations is 2000)
     */
    private static final Integer DEFAULT_ITERATIONS = 2000;

    /**
     * Algorithm name
     */
    private static final String algorithm = "pbkdf2_sha256";

    //private final Logger logger = LoggerFactory.getLogger(this.getClass());    
    private static final Logger logger = LoggerFactory.getLogger(Pbkdf2Sha256.class);
    
    /**
     * Get ciphertext
     * @param password password in plain text
     * @param salt add salt
     * @param iterations number of iterations
     * @return
     */
    public static String getEncodedHash(String password, String salt, int iterations) {
        //Returns only the last part of whole encoded password
        SecretKeyFactory keyFactory = null;
        try {
            keyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
        } catch (NoSuchAlgorithmException e) {
            logger.error("Could NOT retrieve PBKDF2WithHmacSHA256 algorithm", e);
        }
        
        // 솔트는 YVFKgvnG7bs5 이런식으로 표현됨, 
        KeySpec keySpec = new PBEKeySpec(password.toCharArray(), salt.getBytes(Charset.forName("UTF-8")), iterations, HASH_BIT_SIZE);        
        //KeySpec keySpec = new PBEKeySpec(password.toCharArray(), Base64.getDecoder().decode(salt), iterations, HASH_BIT_SIZE);        
        
        SecretKey secret = null;
        try{
            secret =  keyFactory.generateSecret(keySpec);
        } catch (InvalidKeySpecException e) {
            logger.error("Could NOT generate secret key", e);
        }

        //Use Base64 to transcode ciphertext 
        byte[] rawHash = secret.getEncoded();
        byte[] hashBase64 = Base64.getEncoder().encode(rawHash);
        return new String(hashBase64);

        //hexadecimal ciphertext 
        //return toHex(secret.getEncoded());
    }

            

    /**
     * Ciphertext plus salt (get the salt value of the length of'SALT_BYTE_SIZE')
     * @return
     */
    public static String getsalt() {
        //Salt value is composed of ASCII table numbers plus uppercase and lowercase letters 
        int length = SALT_BYTE_SIZE;
        Random rand = new Random();
        char[] rs = new char[length];
        for (int i = 0; i < length; i++) {
            int t = rand.nextInt(3);
            if (t == 0) {
                rs[i] = (char) (rand.nextInt(10) + 48);
            } else if (t == 1) {
                rs[i] = (char) (rand.nextInt(26) + 65);
            } else {
                rs[i] = (char) (rand.nextInt(26)+97);}}
        return new String (rs);
    }    

    /**
     * Get ciphertext
     * The default number of iterations: 2000
     * @param password plain text password
     * @return
     */
    public static String encode(String password) {
        return encode(password, getsalt());
    }

    /**
     * Get ciphertext
     * @param password plain text password
     * @param iterations number of iterations
     * @return
     */
    public static String encode(String password, int iterations) {
        return encode(password, getsalt(), iterations);
    }

    /**
     * Get ciphertext
     * The default number of iterations: 2000
     * @param password plain text password
     * @param salt salt value
     * @return
     */
    public static String encode(String password, String salt) {
        return encode(password, salt, DEFAULT_ITERATIONS);
    }

    /**
     * The entire string of ciphertexts finally returned
     *
     * Note: This method returns the cipher text string composition: algorithm name + iteration number + salt value + cipher text
     * Unneeded ciphertext returned by getEncodedHash method directly
     *
     * @param password password in plain text
     * @param salt add salt
     * @param iterations number of iterations
     * @return
     */
    public static String encode(String password, String salt, int iterations) {
        //returns hashed password, along with algorithm, number of iterations and salt
        String hash = getEncodedHash(password, salt, iterations);
        return String.format("%s$%d$%s$%s", algorithm, iterations, salt, hash);
    }

    /**
     * verify password
     * @param password plaintext
     * @param hashedPassword ciphertext
     * @return
     */
    public static boolean verification(String password, String hashedPassword) {
        //hashedPassword = algorithm name + number of iterations + salt value + ciphertext;
        String[] parts = hashedPassword.split("\\$");
        if (parts.length != 4) {
            return false;
        }
        //Analyze the number of iterations and the salt value to perform the salt value
        Integer iterations = Integer.parseInt(parts[1]);
        String salt = parts[2];
        String hash = encode(password, salt, iterations);
        return hash.equals(hashedPassword);
    }	

}
