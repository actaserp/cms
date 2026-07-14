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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
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

    public JsonNode getInstituteDetail(String spjangcd, String cmsCode) throws Exception {
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
        if (resp.statusCode() == 409) {
            JsonNode errNode = objectMapper.readTree(resp.body());
            String respCode = errNode.path("response_code").asText("");
            String respMsg  = errNode.path("response_message").asText("");
            if ("B0007".equals(respCode)) {
                throw new IllegalStateException("금결원에 오늘 날짜 " + fileType + " 파일이 이미 존재합니다. (" + respMsg + ")");
            }
            throw new IllegalStateException("금결원 API 오류(409): " + resp.body());
        }
        if (resp.statusCode() != 200) {
            throw new IllegalStateException("금결원 API 오류: HTTP " + resp.statusCode() + " " + resp.body());
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

        // 404 = 파일 없음 (불능 0건) — 호출부에서 구분할 수 있도록 별도 예외
        if (resp.statusCode() == 404) {
            throw new CmsFileNotFoundException("수신 파일 없음 fileType=" + fileType + " date=" + transactionDate);
        }
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

    public class CmsFileNotFoundException extends RuntimeException {
        public CmsFileNotFoundException(String message) { super(message); }
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

    public Map<String, Object> checkCenterError(String spjangcd, String fileType, String transactionDate) throws Exception {
        JsonNode root = getCenterError(spjangcd, fileType, transactionDate);
        JsonNode data = root.path("data");

        String vmsg   = data.path("validation_message").asText("");
        int requested = data.path("requested_total_record_count").asInt(0);
        int valid     = data.path("valid_count").asInt(0);

        List<Map<String, Object>> details = new java.util.ArrayList<>();
        JsonNode arr = data.path("center_error_details");
        if (arr.isArray()) {
            for (JsonNode e : arr) {
                Map<String, Object> d = new java.util.HashMap<>();
                d.put("recordNo",     e.path("record_no").asText(""));
                d.put("payerNo",      e.path("payer_no").asText(""));
                d.put("errorCode",    e.path("error_code").asText(""));
                d.put("errorMessage", e.path("error_message").asText(""));
                details.add(d);
            }
        }

        Map<String, Object> result = new java.util.HashMap<>();
        result.put("validationMessage", vmsg);   // Y: 상세조회 가능 / N: 전체정상 / R: 결과파일(EB22) 단계
        result.put("requestedCount",    requested);
        result.put("validCount",        valid);
        result.put("errorCount",        details.isEmpty() ? Math.max(requested - valid, 0) : details.size());
        result.put("details",           details);
        result.put("responseCode",      root.path("response_code").asText(""));
        result.put("responseMessage",   root.path("response_message").asText(""));
        return result;
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

    // 파일 목록 조회 (최대 1개월)
    public JsonNode getFileList(String spjangcd, String fileType) throws Exception {
        String token     = getToken(spjangcd);
        String startDate = LocalDate.now().minusMonths(1).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String endDate   = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String url = apiBaseUrl + "/biz/batch/states?search_start_date=" + startDate
                + "&search_end_date=" + endDate
                + "&file_type=" + fileType;

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Bearer " + token)
                .GET()
                .build();

        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        log.info("[CmsFileList] fileType={} startDate={} endDate={} 응답={}", fileType, startDate, endDate, resp.body());

        if (resp.statusCode() == 404) return null;
        if (resp.statusCode() != 200) {
            throw new IllegalStateException("파일 목록 조회 실패: HTTP " + resp.statusCode() + " " + resp.body());
        }
        return objectMapper.readTree(resp.body());
    }

    private String str(Object v) { return v != null ? v.toString() : ""; }

    // ── 실시간 계좌조회 (DEPOSITOR_NAME_INQUIRY) ──────────────
    //
    // 금결원 실시간 부가서비스 엔드포인트: POST {apiBaseUrl}/biz/realtime/transaction
    // 요청 형식이 다른 API와 달리 multipart/form-data 이고,
    // 'request' 파트에 JSON 문자열(type=application/json)을 담아야 한다.
    // 계좌조회는 evidence_file 을 넣지 않는다.
    //
    // 주의: 이 호출은 "예금주명 확인"만 한다. 출금이체 등록(신청) 여부와는 무관하다.
    //
    // 반환: 금결원 응답의 data 노드 (account_depositor_name, response_code 등 포함)
    public JsonNode realtimeAccountInquiry(String spjangcd,
                                           String bankCode,
                                           String accountNo,
                                           String identificationNo,
                                           long instituteTrackingNo) throws Exception {
        String token = getToken(spjangcd);

        // request JSON (계좌조회 필수 필드만)
        String requestJson = objectMapper.writeValueAsString(Map.of(
                "account_no", accountNo,
                "bank_code", bankCode,
                "identification_no", identificationNo,
                "institute_tracking_no", instituteTrackingNo,
                "realtime_transaction_type", "DEPOSITOR_NAME_INQUIRY"
        ));

        // multipart/form-data 본문 직접 구성 (Java 표준 HttpClient는 multipart 빌더가 없음)
        String boundary = "----CmsBoundary" + System.currentTimeMillis();
        String CRLF = "\r\n";
        StringBuilder sb = new StringBuilder();
        sb.append("--").append(boundary).append(CRLF);
        sb.append("Content-Disposition: form-data; name=\"request\"").append(CRLF);
        sb.append("Content-Type: application/json").append(CRLF);
        sb.append(CRLF);
        sb.append(requestJson).append(CRLF);
        sb.append("--").append(boundary).append("--").append(CRLF);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(apiBaseUrl + "/biz/realtime/transaction"))
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofString(sb.toString()))
                .build();

        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        log.info("[CmsRealtimeInquiry] spjangcd={} bankCode={} accountNo={} trackingNo={} status={} 응답={}",
                spjangcd, bankCode, accountNo, instituteTrackingNo, resp.statusCode(), resp.body());

        // 처리중(102) — 실시간 요청내역 조회로 재확인해야 하는 케이스. 여기선 그대로 올려 호출부에서 판단.
        if (resp.statusCode() != 200 && resp.statusCode() != 102) {
            throw new IllegalStateException("실시간 계좌조회 실패: HTTP " + resp.statusCode() + " " + resp.body());
        }

        JsonNode node = objectMapper.readTree(resp.body());
        // 최상위 response_code 는 요청 접수 코드(B0000 계열). 실제 조회 결과는 data.response_code 를 봐야 함.
        return node.path("data");
    }

}