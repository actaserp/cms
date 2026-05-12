package mes.app.cms.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class CmsTokenService {

    private final Map<String, String>  tokenCache     = new ConcurrentHashMap<>();
    private final Map<String, Instant> tokenExpireMap = new ConcurrentHashMap<>();

    private final SqlRunner sqlRunner;
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${cms.api-base-url}")
    private String apiBaseUrl;

    @Value("${cms.client-id}")
    private String clientId;

    @Value("${cms.client-secret}")
    private String clientSecret;

    public CmsTokenService(SqlRunner sqlRunner) {
        this.sqlRunner = sqlRunner;
    }

    public synchronized String getToken(String spjangcd) throws Exception {
        Instant expireAt = tokenExpireMap.getOrDefault(spjangcd, Instant.EPOCH);
        String cached = tokenCache.get(spjangcd);

        if (cached != null && Instant.now().isBefore(expireAt.minusSeconds(600))) {
            log.info("[CmsToken] 캐시 토큰 사용 spjangcd={} 토큰={}", spjangcd, cached);
            return cached;
        }

        Map<String, Object> xa012 = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT cms_code FROM tb_xa012_cms WHERE spjangcd = :spjangcd",
                new MapSqlParameterSource("spjangcd", spjangcd));
        String institutionCode = xa012 != null ? str(xa012.get("cms_code")) : "";
        if (!StringUtils.hasText(institutionCode)) {
            throw new IllegalStateException("cms_code 미설정 spjangcd=" + spjangcd);
        }

        String body = "grant_type=client_credentials"
                + "&client_id=" + clientId
                + "&client_secret=" + clientSecret
                + "&inst_code=" + institutionCode
                + "&scope=CMS_INSTITUTE";

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(apiBaseUrl + "/auth/token"))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new IllegalStateException("토큰 발급 실패: HTTP " + resp.statusCode() + " " + resp.body());
        }

        JsonNode node = objectMapper.readTree(resp.body());
        String token = node.path("access_token").asText(null);
        if (!StringUtils.hasText(token)) {
            throw new IllegalStateException("토큰 발급 응답에 access_token 없음: " + resp.body());
        }
        long expiresIn = node.path("expires_in").asLong(86037);
        tokenCache.put(spjangcd, token);
        tokenExpireMap.put(spjangcd, Instant.now().plusSeconds(expiresIn));
        log.info("[CmsToken] 토큰 발급 완료 spjangcd={} 유효시간={}s  토큰={}", spjangcd, expiresIn, token);
        return token;
    }

    // ── 이용기관 상세 정보 조회 ───────────────────────────────

    public JsonNode getInstituteDetail(String spjangcd) throws Exception {
        String token = getToken(spjangcd);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(apiBaseUrl + "/biz/institute/detail"))
                .header("Authorization", "Bearer " + token)
                .GET()
                .build();
        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        log.info("[CmsInstituteDetail] spjangcd={} 응답={}", spjangcd, resp.body());
        if (resp.statusCode() != 200) {
            throw new IllegalStateException("이용기관 상세 조회 실패: HTTP " + resp.statusCode() + " " + resp.body());
        }
        JsonNode node = objectMapper.readTree(resp.body());
        String respCode = node.path("response_code").asText("");
        if (!"B0000".equals(respCode)) {
            throw new IllegalStateException("이용기관 상세 조회 오류: " + respCode + " " + node.path("response_message").asText());
        }
        return node.path("data");
    }

    // ── SFTP 송신 권한 ────────────────────────────────────────

    public String[] getSftpSendCredential(String spjangcd, String fileType, String transactionDate) throws Exception {
        String token = getToken(spjangcd);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(apiBaseUrl + "/biz/batch?file_type=" + fileType + "&transaction_date=" + transactionDate))
                .header("Authorization", "Bearer " + token)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new IllegalStateException("SFTP 송신 권한 요청 실패: HTTP " + resp.statusCode() + " " + resp.body());
        }

        JsonNode node = objectMapper.readTree(resp.body());
        String respCode = node.path("response_code").asText("");
        if (!"B0000".equals(respCode)) {
            throw new IllegalStateException("SFTP 송신 권한 오류: " + respCode + " " + node.path("response_message").asText());
        }

        JsonNode data = node.path("data");
        return new String[]{ data.path("sftp_user_name").asText(), data.path("sftp_password").asText() };
    }

    // ── SFTP 수신 권한 ────────────────────────────────────────

    public String[] getSftpReceiveCredential(String spjangcd, String fileType, String transactionDate) throws Exception {
        String token = getToken(spjangcd);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(apiBaseUrl + "/biz/batch?file_type=" + fileType + "&transaction_date=" + transactionDate))
                .header("Authorization", "Bearer " + token)
                .GET()
                .build();

        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new IllegalStateException("SFTP 수신 권한 요청 실패: HTTP " + resp.statusCode() + " " + resp.body());
        }

        JsonNode node = objectMapper.readTree(resp.body());
        String respCode = node.path("response_code").asText("");
        if (!"B0000".equals(respCode)) {
            throw new IllegalStateException("SFTP 수신 권한 오류: " + respCode + " " + node.path("response_message").asText());
        }

        JsonNode data = node.path("data");
        return new String[]{ data.path("sftp_user_name").asText(), data.path("sftp_password").asText() };
    }

    // ── 파일 상태 확인 ────────────────────────────────────────

    public JsonNode getFileStatus(String spjangcd, String fileType, String transactionDate) throws Exception {
        return getFileStatus(spjangcd, fileType, transactionDate, false);
    }

    public JsonNode getFileStatus(String spjangcd, String fileType, String transactionDate, boolean waitForResult) throws Exception {
        String token = getToken(spjangcd);
        String url = apiBaseUrl + "/biz/batch/state?file_type=" + fileType
                + "&transaction_date=" + transactionDate
                + (waitForResult ? "&wait_for_result=true" : "");
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Bearer " + token)
                .GET()
                .build();
        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        log.info("[CmsFileStatus] fileType={} date={} waitForResult={} 응답={}", fileType, transactionDate, waitForResult, resp.body());
        return objectMapper.readTree(resp.body());
    }

    // ── 센터오류 상세 조회 ────────────────────────────────────

    public JsonNode getCenterError(String spjangcd, String fileType, String transactionDate) throws Exception {
        String token = getToken(spjangcd);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(apiBaseUrl + "/biz/batch/error?file_type=" + fileType + "&transaction_date=" + transactionDate))
                .header("Authorization", "Bearer " + token)
                .GET()
                .build();
        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        log.info("[CmsCenterError] fileType={} date={} 응답={}", fileType, transactionDate, resp.body());
        return objectMapper.readTree(resp.body());
    }

    // ── 파일 전송 취소 ────────────────────────────────────────

    public boolean cancelFile(String spjangcd, String fileType, String transactionDate) throws Exception {
        String token = getToken(spjangcd);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(apiBaseUrl + "/biz/batch?file_type=" + fileType + "&transaction_date=" + transactionDate))
                .header("Authorization", "Bearer " + token)
                .PUT(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        log.info("[CmsFileCancel] fileType={} date={} 응답={}", fileType, transactionDate, resp.body());
        JsonNode node = objectMapper.readTree(resp.body());
        return "B0000".equals(node.path("response_code").asText(""));
    }

    // ── 이용기관 상세 정보 조회 (cmsCode 직접) ──────────────

    public JsonNode getInstituteDetailByCode(String cmsCode) throws Exception {
        String token = getTokenByCode(cmsCode);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(apiBaseUrl + "/biz/institute/detail"))
                .header("Authorization", "Bearer " + token)
                .GET()
                .build();
        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        log.info("[CmsInstituteDetailByCode] cmsCode={} 응답={}", cmsCode, resp.body());
        if (resp.statusCode() != 200) {
            throw new IllegalStateException("이용기관 상세 조회 실패: HTTP " + resp.statusCode() + " " + resp.body());
        }
        JsonNode node = objectMapper.readTree(resp.body());
        String respCode = node.path("response_code").asText("");
        if (!"B0000".equals(respCode)) {
            throw new IllegalStateException("이용기관 상세 조회 오류: " + respCode + " " + node.path("response_message").asText());
        }
        return node.path("data");
    }

    private String getTokenByCode(String cmsCode) throws Exception {
        String body = "grant_type=client_credentials"
                + "&client_id=" + clientId
                + "&client_secret=" + clientSecret
                + "&inst_code=" + cmsCode
                + "&scope=CMS_INSTITUTE";

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(apiBaseUrl + "/auth/token"))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new IllegalStateException("토큰 발급 실패: HTTP " + resp.statusCode() + " " + resp.body());
        }
        JsonNode node = objectMapper.readTree(resp.body());
        String token = node.path("access_token").asText(null);
        if (!StringUtils.hasText(token)) {
            throw new IllegalStateException("토큰 발급 응답에 access_token 없음: " + resp.body());
        }
        log.info("[CmsToken] 기관코드 직접 토큰 발급 완료 cmsCode={}", cmsCode);
        return token;
    }

    private String str(Object v) { return v != null ? v.toString() : ""; }
}