package mes.app.naverCloud.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

@Service
public class NcpAuthService {

    @Value("${ncp_api_accessKey}")
    private String accessKey;
    @Value("${ncp_api_secretKey}")
    private String secretKey;


    public HttpHeaders createHeader(HttpMethod method, String apiPath, String timestamp){
        String signature = makeSignature(method.name(), apiPath, timestamp);
        HttpHeaders headers = new HttpHeaders();
        headers.set("x-ncp-apigw-timestamp", timestamp);
        headers.set("x-ncp-iam-access-key", accessKey);
        headers.set("x-ncp-apigw-signature-v2", signature);
        headers.setContentType(MediaType.APPLICATION_JSON);
        return headers;
    }

    private String makeSignature(String method, String url, String timestamp) {
        try {
            String message = new StringBuilder()
                    .append(method).append(" ").append(url).append("\n")
                    .append(timestamp).append("\n")
                    .append(accessKey)
                    .toString();

            SecretKeySpec signingKey = new SecretKeySpec(secretKey.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(signingKey);

            byte[] rawHmac = mac.doFinal(message.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(rawHmac);
        } catch (Exception e) {
            throw new RuntimeException("Signature 생성 실패", e);
        }
    }
}
