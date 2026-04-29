package mes.app.Scheduler.SchedulerService;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jcraft.jsch.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mes.app.cms.service.CmsBillingService;
import mes.app.files.NcpObjectStorageService;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * D-1 15:00 실행 — 익일 출금 PENDING 청구 → EB21 생성 + NCP 업로드 + SFTP 전송
 * billing.status: PENDING → REQUESTED
 *
 * SFTP 연동 방식: 금결원 오픈API로 1회용 SFTP 계정 획득 후 접속
 *   - 송신: POST /biz/batch?file_type=EB21&transaction_date=YYYYMMDD
 *   - SFTP 호스트: sftp.cmsedi.or.kr:11133 (운영) / tsftp.cmsedi.or.kr:11133 (테스트)
 *   - 파일명: EB21{MMDD}_{YYYY} (연도 suffix 필수)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CmsEb21SendService {

    private static final String FEATURE_CODE = "EB_FILE";
    private static final Charset EUC_KR = Charset.forName("EUC-KR");

    private final SqlRunner sqlRunner;
    private final NcpObjectStorageService storageService;
    private final CmsBillingService cmsBillingService;
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    // 토큰 캐시 (23시간57분 유효 — 만료 10분 전 재발급)
    private final AtomicReference<String>  cachedToken     = new AtomicReference<>();
    private final AtomicReference<Instant> tokenExpireAt   = new AtomicReference<>(Instant.EPOCH);

    @Value("${cms.institution-code}")
    private String institutionCode;

    @Value("${cms.api-base-url}")
    private String apiBaseUrl;

    @Value("${cms.client-id}")
    private String clientId;

    @Value("${cms.client-secret}")
    private String clientSecret;

    @Value("${cms.sftp-host:tsftp.cmsedi.or.kr}")
    private String sftpHost;

    @Value("${cms.sftp-port:11133}")
    private int sftpPort;

    public void run() {
        String targetDate = LocalDate.now().plusDays(1)
                .format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        log.info("[CmsEbFileGenerate] 시작 - 출금대상일: {}", targetDate);

        List<Map<String, Object>> spjangs = sqlRunner.getRows(/* skip_tenant_check */
                "SELECT DISTINCT spjangcd FROM cms_billing WHERE deduct_date = :td AND status = 'PENDING' AND deduct_type = 'EB'",
                new MapSqlParameterSource("td", targetDate));

        log.info("[CmsEbFileGenerate] 대상 사업장 수: {}", spjangs.size());

        for (Map<String, Object> row : spjangs) {
            String spjangcd = (String) row.get("spjangcd");
            try {
                generateAndSend(spjangcd, targetDate);
            } catch (Exception e) {
                log.error("[CmsEbFileGenerate] 실패 spjangcd={}: {}", spjangcd, e.getMessage(), e);
            }
        }
        log.info("[CmsEbFileGenerate] 완료");
    }

    /** 수동 생성(화면)용 */
    public Map<String, Object> runForSpjang(String spjangcd, String targetDate, String userId) {
        var result = new java.util.HashMap<String, Object>();
        try {
            long fileId = generateAndSend(spjangcd, targetDate);
            result.put("file_id", fileId);
        } catch (Exception e) {
            result.put("error", e.getMessage());
        }
        return result;
    }

    private long generateAndSend(String spjangcd, String targetDate) throws Exception {
        // 1. PENDING 청구 조회
        var param = new MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);
        param.addValue("targetDate", targetDate);

        List<Map<String, Object>> billings = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT b.id, b.bank_code, b.bank_account, b.account_holder, b.billing_amount,
                       m.id_number, m.member_no, m.phone
                FROM cms_billing b
                LEFT JOIN cms_member m ON m.id = b.member_id
                WHERE b.spjangcd    = :spjangcd
                  AND b.deduct_date = :targetDate
                  AND b.status      = 'PENDING'
                  AND b.deduct_type = 'EB'
                ORDER BY b.id
                """, param);

        if (billings.isEmpty()) throw new IllegalStateException("PENDING 청구 건 없음");

        // 필수 요소 검증 — 누락 건은 ERROR 처리, 유효 건만 파일에 포함
        List<Long> invalidIds = new java.util.ArrayList<>();
        List<Map<String, Object>> validBillings = new java.util.ArrayList<>();
        for (Map<String, Object> b : billings) {
            if (StringUtils.hasText(str(b.get("bank_code")))
                    && StringUtils.hasText(str(b.get("bank_account")))
                    && StringUtils.hasText(str(b.get("account_holder")))
                    && b.get("billing_amount") != null) {
                validBillings.add(b);
            } else {
                invalidIds.add(((Number) b.get("id")).longValue());
                log.warn("[CmsEb21] 필수 항목 누락 billing_id={}", b.get("id"));
            }
        }
        if (!invalidIds.isEmpty()) {
            cmsBillingService.updateStatusToError(invalidIds, "필수 항목 누락(은행코드/계좌/예금주/금액)");
            log.warn("[CmsEb21] 필수 항목 누락 ERROR 처리: {}건", invalidIds.size());
        }
        if (validBillings.isEmpty()) throw new IllegalStateException("유효한 PENDING 청구 건 없음");
        billings = validBillings;

        // 2. EB21 생성
        byte[] fileBytes = buildEb21File(spjangcd, billings);
        // 파일명: EB21{MMDD}_{YYYY} (금결원 규격)
        String mmdd     = targetDate.substring(4, 8);
        String yyyy     = targetDate.substring(0, 4);
        String fileName = "EB21" + mmdd + "_" + yyyy;
        String objectKey = storageService.buildObjectKey(spjangcd, FEATURE_CODE, fileName);

        // 3. NCP 업로드
        try (ByteArrayInputStream bis = new ByteArrayInputStream(fileBytes)) {
            storageService.upload(objectKey, bis, fileBytes.length, "text/plain");
        }

        // 4. cms_file INSERT
        long totalAmount = billings.stream()
                .mapToLong(b -> b.get("billing_amount") != null ? ((Number) b.get("billing_amount")).longValue() : 0L)
                .sum();

        var fp = new MapSqlParameterSource();
        fp.addValue("spjangcd",      spjangcd);
        fp.addValue("fileName",      fileName);
        fp.addValue("filePath",      objectKey);
        fp.addValue("targetDate",    targetDate);
        fp.addValue("billingCount",  billings.size());
        fp.addValue("billingAmount", totalAmount);

        Map<String, Object> cmsFileRow = sqlRunner.getRow(/* skip_tenant_check */
                """
                INSERT INTO cms_file (
                    spjangcd, file_name, file_type, file_path,
                    target_date, billing_count, billing_amount,
                    send_status, _creater_id, _created, _modifier_id, _modified
                ) VALUES (
                    :spjangcd, :fileName, 'EB_REQUEST', :filePath,
                    CAST(:targetDate AS DATE), :billingCount, :billingAmount,
                    'PENDING', 'SYSTEM', NOW(), 'SYSTEM', NOW()
                ) RETURNING id
                """, fp);

        if (cmsFileRow == null) throw new IllegalStateException("cms_file INSERT 실패");
        long fileId = ((Number) cmsFileRow.get("id")).longValue();

        // 5. SFTP 전송 (1회용 계정 획득 후)
        boolean sent = false;
        String errMsg = null;
        try {
            sftpSendWithApiCredential(fileBytes, fileName, targetDate);
            sent = true;
        } catch (Exception e) {
            errMsg = e.getMessage();
            log.error("[CmsEbFileGenerate] SFTP 전송 실패 spjangcd={}: {}", spjangcd, e.getMessage());
        }

        // 6. cms_file 상태 업데이트
        var up = new MapSqlParameterSource("id", fileId);
        if (sent) {
            sqlRunner.execute(/* skip_tenant_check */
                    "UPDATE cms_file SET send_status='SENT', send_type='SFTP', sent_at=NOW(), _modified=NOW() WHERE id=:id", up);
        } else {
            up.addValue("errMsg", errMsg);
            sqlRunner.execute(/* skip_tenant_check */
                    "UPDATE cms_file SET send_status='FAILED', error_message=:errMsg, _modified=NOW() WHERE id=:id", up);
        }

        // 7. cms_file_billing 매핑 INSERT (line_seq 부여, 1건씩)
        int seq = 1;
        List<Long> billingIds = new java.util.ArrayList<>();
        for (Map<String, Object> b : billings) {
            long billingId = ((Number) b.get("id")).longValue();
            billingIds.add(billingId);
            var mp = new MapSqlParameterSource();
            mp.addValue("fileId", fileId);
            mp.addValue("billingId", billingId);
            mp.addValue("lineSeq", seq++);
            sqlRunner.execute(/* skip_tenant_check */
                    "INSERT INTO cms_file_billing(file_id,billing_id,line_seq,_created) VALUES(:fileId,:billingId,:lineSeq,NOW()) ON CONFLICT(billing_id) DO UPDATE SET file_id=EXCLUDED.file_id,line_seq=EXCLUDED.line_seq",
                    mp);
        }

        // 8. billing 상태 업데이트
        if (sent) {
            int updatedCount = cmsBillingService.updateStatusToRequested(billingIds, fileId);
            log.info("[CmsEb21FileGenerate] billing REQUESTED 전환: {}건", updatedCount);
        } else {
            cmsBillingService.updateStatusToError(billingIds, errMsg);
            log.warn("[CmsEb21FileGenerate] billing ERROR 전환: {}건", billingIds.size());
        }

        log.info("[CmsEbFileGenerate] spjangcd={} 파일={} {}건 {}원 SFTP={}",
                spjangcd, fileName, billings.size(), totalAmount, sent ? "OK" : "FAILED");
        return fileId;
    }

    /** 수동 SFTP 재전송 (CmsEbFileService에서 호출) */
    public void sftpSendBytes(byte[] fileBytes, String fileName, String targetDate) throws Exception {
        sftpSendWithApiCredential(fileBytes, fileName, targetDate);
    }

    /** EB21 재시도 — ERROR 건을 PENDING으로 리셋 후 재전송 (스케줄러 호출) */
    public void retry(String targetDate) {
        List<Map<String, Object>> spjangs = sqlRunner.getRows(/* skip_tenant_check */
                "SELECT DISTINCT spjangcd FROM cms_billing WHERE deduct_date=:td AND status='ERROR' AND deduct_type='EB'",
                new MapSqlParameterSource("td", targetDate));

        if (spjangs.isEmpty()) {
            log.info("[CmsEb21Retry] ERROR 건 없음 - 재시도 종료");
            return;
        }
        log.info("[CmsEb21Retry] 재시도 시작 - 대상 사업장: {}", spjangs.size());

        for (Map<String, Object> row : spjangs) {
            String spjangcd = (String) row.get("spjangcd");
            sqlRunner.execute(/* skip_tenant_check */
                    "UPDATE cms_billing SET status='PENDING',result_msg=NULL,_modified=NOW() WHERE spjangcd=:s AND deduct_date=:td AND status='ERROR' AND deduct_type='EB'",
                    new MapSqlParameterSource("s", spjangcd).addValue("td", targetDate));
            try {
                generateAndSend(spjangcd, targetDate);
            } catch (Exception e) {
                log.error("[CmsEb21Retry] 실패 spjangcd={}: {}", spjangcd, e.getMessage(), e);
            }
        }
        log.info("[CmsEb21Retry] 재시도 완료");
    }

    /** 사용자 수동 재전송 — PENDING 건 선택 후 재전송 */
    public Map<String, Object> resendBilling(List<Long> billingIds) {
        List<Map<String, Object>> groups = sqlRunner.getRows(/* skip_tenant_check */
                "SELECT DISTINCT spjangcd, deduct_date FROM cms_billing WHERE id IN (:ids) AND status='PENDING' AND deduct_type='EB'",
                new MapSqlParameterSource("ids", billingIds));

        int sent = 0, failed = 0;
        for (Map<String, Object> g : groups) {
            String spjangcd  = (String) g.get("spjangcd");
            String targetDate = (String) g.get("deduct_date");
            try {
                Map<String, Object> failedFile = sqlRunner.getRow(/* skip_tenant_check */
                        """
                        SELECT id, file_path, file_name FROM cms_file
                        WHERE spjangcd=:s AND target_date=CAST(:td AS DATE) AND file_type='EB_REQUEST' AND send_status='FAILED'
                        ORDER BY id DESC LIMIT 1
                        """,
                        new MapSqlParameterSource("s", spjangcd).addValue("td", targetDate));

                if (failedFile != null) {
                    long   fileId   = ((Number) failedFile.get("id")).longValue();
                    String filePath = (String) failedFile.get("file_path");
                    String fileName = (String) failedFile.get("file_name");

                    byte[] fileBytes;
                    try (var stream = storageService.download(filePath)) {
                        fileBytes = stream.readAllBytes();
                    }
                    sftpSendWithApiCredential(fileBytes, fileName, targetDate);

                    sqlRunner.execute(/* skip_tenant_check */
                            "UPDATE cms_file SET send_status='SENT',send_type='SFTP',sent_at=NOW(),_modified=NOW() WHERE id=:id",
                            new MapSqlParameterSource("id", fileId));

                    List<Long> linkedIds = sqlRunner.getRows(/* skip_tenant_check */
                            "SELECT cfb.billing_id FROM cms_file_billing cfb JOIN cms_billing cb ON cb.id=cfb.billing_id WHERE cfb.file_id=:fid AND cb.status='PENDING'",
                            new MapSqlParameterSource("fid", fileId))
                            .stream().map(r -> ((Number) r.get("billing_id")).longValue())
                            .collect(java.util.stream.Collectors.toList());

                    if (!linkedIds.isEmpty()) cmsBillingService.updateStatusToRequested(linkedIds, fileId);
                    log.info("[CmsEb21Resend] 기존 파일 재전송 성공: spjangcd={} file={}", spjangcd, fileName);
                } else {
                    generateAndSend(spjangcd, targetDate);
                    log.info("[CmsEb21Resend] 파일 새로 생성+전송: spjangcd={} targetDate={}", spjangcd, targetDate);
                }
                sent++;
            } catch (Exception e) {
                failed++;
                log.error("[CmsEb21Resend] 실패 spjangcd={}: {}", spjangcd, e.getMessage(), e);
            }
        }

        var result = new java.util.HashMap<String, Object>();
        result.put("sent", sent);
        result.put("failed", failed);
        return result;
    }

    // ── OAuth2 토큰 관리 ───────────────────────────────────────────────────────

    private synchronized String getToken() throws Exception {
        // 만료 10분 전에 재발급
        if (cachedToken.get() != null && Instant.now().isBefore(tokenExpireAt.get().minusSeconds(600))) {
            return cachedToken.get();
        }
        String body = "grant_type=client_credentials"
                + "&client_id=" + clientId
                + "&client_secret=" + clientSecret
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
        long expiresIn = node.path("expires_in").asLong(86037); // 23h57m 기본
        cachedToken.set(token);
        tokenExpireAt.set(Instant.now().plusSeconds(expiresIn));
        log.info("[CmsEbFile] 토큰 발급 완료, 유효시간={}s", expiresIn);
        return token;
    }

    // ── SFTP 1회용 계정 획득 + 전송 ───────────────────────────────────────────

    private void sftpSendWithApiCredential(byte[] fileBytes, String fileName, String targetDate) throws Exception {
        String token = getToken();

        // 파일 송신 권한 요청 → 1회용 SFTP 계정
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(apiBaseUrl + "/biz/batch?file_type=EB21&transaction_date=" + targetDate))
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
        String sftpUser = data.path("sftp_user_name").asText();
        String sftpPass = data.path("sftp_password").asText();

        log.info("[CmsEbFile] SFTP 1회용 계정 획득: user={}", sftpUser);
        sftpUpload(fileBytes, fileName, sftpUser, sftpPass);
    }

    /** SFTP 파일 수신용 1회용 계정 획득 (CmsEb22ReceiveService에서 호출) */
    public String[] getSftpReceiveCredential(String fileType, String transactionDate) throws Exception {
        String token = getToken();

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

    private void sftpUpload(byte[] fileBytes, String fileName, String user, String password)
            throws JSchException, SftpException {
        JSch jsch = new JSch();
        Session session = jsch.getSession(user, sftpHost, sftpPort);
        session.setPassword(password);
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);
        session.connect(15000);

        ChannelSftp channel = (ChannelSftp) session.openChannel("sftp");
        channel.connect(10000);
        try {
            channel.put(new ByteArrayInputStream(fileBytes), fileName);
            log.info("[CmsEbFile] SFTP 업로드 완료: {}", fileName);
        } catch (SftpException e) {
            // 금결원 서버가 파일 수신 직후 강제 접속 종료 → EOF 에러는 정상으로 처리
            if (e.getMessage() != null && e.getMessage().contains("End of IO")) {
                log.warn("[CmsEbFile] SFTP 서버 강제종료(정상): {}", e.getMessage());
            } else {
                throw e;
            }
        } finally {
            try { channel.disconnect(); } catch (Exception ignored) {}
            try { session.disconnect(); } catch (Exception ignored) {}
        }
    }

    // ── EB21 파일 포맷 ─────────────────────────────────────────────────────────

    private byte[] buildEb21File(String spjangcd, List<Map<String, Object>> billings) throws IOException {
        String orgCode = padRight(institutionCode, 10);

        Map<String, Object> spjang = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT cms_bank_branch, cms_recv_account, cms_description FROM tb_xa012 WHERE spjangcd=:s",
                new MapSqlParameterSource("s", spjangcd));

        String bankBranch = padRight(spjang != null ? str(spjang.get("cms_bank_branch"))  : "", 7);
        String recvAcct   = padRight(spjang != null ? str(spjang.get("cms_recv_account")) : "", 16);
        String desc       = toEb21Desc(spjang != null ? str(spjang.get("cms_description")) : "");

        long totalAmount = 0;
        var baos = new ByteArrayOutputStream();

        // Header
        StringBuilder h = new StringBuilder();
        h.append("H").append("00000000").append(orgCode)
         .append(bankBranch).append(recvAcct).append(spaces(108));
        baos.write(toEucKr150(h.toString())); baos.write('\n');

        // Data Records
        int seq = 1;
        for (Map<String, Object> b : billings) {
            long amount = b.get("billing_amount") != null ? ((Number) b.get("billing_amount")).longValue() : 0L;
            totalAmount += amount;

            StringBuilder r = new StringBuilder();
            r.append("R")
             .append(padLeft(String.valueOf(seq++), 8, '0'))
             .append(orgCode)
             .append(padRight(str(b.get("bank_code")), 3)).append("0000")
             .append(padLeft(str(b.get("bank_account")).replaceAll("-", ""), 16))
             .append(padLeft(String.valueOf(amount), 13, '0'))
             .append(padRight(str(b.get("id_number")), 13))
             .append(spaces(5))
             .append(desc)
             .append("  ")
             .append(padRight(str(b.get("member_no")), 20))
             .append(spaces(5))
             .append("1")
             .append(padRight(str(b.get("phone")).replaceAll("[^0-9]", ""), 12))
             .append(spaces(21));
            baos.write(toEucKr150(r.toString())); baos.write('\n');
        }

        // Trailer
        StringBuilder t = new StringBuilder();
        t.append("T").append("99999999").append(orgCode)
         .append(padLeft(String.valueOf(seq - 1), 6, '0'))
         .append(padLeft(String.valueOf(totalAmount), 15, '0'))
         .append(spaces(110));
        baos.write(toEucKr150(t.toString()));

        return baos.toByteArray();
    }

    private String toEb21Desc(String desc) {
        if (!StringUtils.hasText(desc)) return spaces(16);
        byte[] b = desc.getBytes(EUC_KR);
        if (b.length >= 16) return new String(Arrays.copyOf(b, 16), EUC_KR);
        byte[] padded = new byte[16];
        Arrays.fill(padded, (byte) ' ');
        System.arraycopy(b, 0, padded, 0, b.length);
        return new String(padded, EUC_KR);
    }

    private byte[] toEucKr150(String s) {
        byte[] b = s.getBytes(EUC_KR);
        if (b.length == 150) return b;
        byte[] result = new byte[150];
        Arrays.fill(result, (byte) ' ');
        System.arraycopy(b, 0, result, 0, Math.min(b.length, 150));
        return result;
    }

    private String str(Object v)             { return v != null ? v.toString() : ""; }
    private String spaces(int n)             { return " ".repeat(n); }
    private String padRight(String s, int n) { return String.format("%-" + n + "s", s != null ? s : "").substring(0, n); }
    private String padLeft(String s, int n, char c) {
        String v = s != null ? s : "";
        if (v.length() >= n) return v.substring(v.length() - n);
        return String.valueOf(c).repeat(n - v.length()) + v;
    }
    private String padLeft(String s, int n) { return padLeft(s, n, ' '); }
}
