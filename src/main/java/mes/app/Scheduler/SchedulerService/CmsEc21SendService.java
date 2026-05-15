package mes.app.Scheduler.SchedulerService;

import com.fasterxml.jackson.databind.JsonNode;
import com.jcraft.jsch.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mes.app.cms.service.CmsBillingService;
import mes.app.cms.service.CmsTokenService;
import mes.app.files.NcpObjectStorageService;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 자동시 D 11:00 실행 — 당일 출금 PENDING 청구 → EC21 생성 + NCP 업로드 + SFTP 전송
 * billing.status: PENDING → REQUESTED
 *
 * SFTP 연동 방식: 금결원 오픈API로 1회용 SFTP 계정 획득 후 접속
 *   - 송신: POST /biz/batch?file_type=EC21&transaction_date=YYYYMMDD
 *   - SFTP 호스트: sftp.cmsedi.or.kr:11133 (운영) / tsftp.cmsedi.or.kr:11133 (테스트)
 *   - 파일명: EC21{MMDD}_{YYYY} (연도 suffix 필수)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CmsEc21SendService {

    private static final String FEATURE_CODE = "EC21";
    private static final Charset EUC_KR = Charset.forName("EUC-KR");

    private final SqlRunner sqlRunner;
    private final NcpObjectStorageService storageService;
    private final CmsBillingService cmsBillingService;
    private final CmsTokenService cmsTokenService;

    @Value("${cms.sftp-host}")
    private String sftpHost;

    @Value("${cms.sftp-port}")
    private int sftpPort;

    public void run() {
        String targetDate = LocalDate.now()
                .format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        log.info("[CmsEc21FileGenerate] 시작 - 출금대상일: {}", targetDate);

        List<Map<String, Object>> spjangs = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT DISTINCT b.spjangcd 
                FROM cms_billing b
                JOIN tb_xa012_cms c ON c.spjangcd = b.spjangcd
                WHERE b.deduct_date = :td 
                  AND b.status = 'PENDING' 
                  AND b.deduct_type = 'EC'
                  AND c.auto_send_yn = 'Y'
                """,
                new MapSqlParameterSource("td", targetDate));

        log.info("[CmsEc21FileGenerate] 대상 사업장 수: {}", spjangs.size());

        for (Map<String, Object> row : spjangs) {
            String spjangcd = (String) row.get("spjangcd");
            try {
                generateAndSend(spjangcd, targetDate);
            } catch (Exception e) {
                log.error("[CmsEc21FileGenerate] 실패 spjangcd={}: {}", spjangcd, e.getMessage(), e);
            }
        }
        log.info("[CmsEc21FileGenerate] 완료");
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
        // institutionCode 조회
        Map<String, Object> xa012Row = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT cms_code, ec21_fee_request, ec21_fee_success FROM tb_xa012_cms WHERE spjangcd=:s",
                new MapSqlParameterSource("s", spjangcd));
        String institutionCode = xa012Row != null ? str(xa012Row.get("cms_code")) : "";
        int feeRequest = xa012Row != null && xa012Row.get("ec21_fee_request") != null
                ? ((Number) xa012Row.get("ec21_fee_request")).intValue() : 0;
        if (!StringUtils.hasText(institutionCode)) {
            log.error("[CmsEc21] cms_code 없음 spjangcd={}", spjangcd);
            throw new IllegalStateException("cms_code 미설정 spjangcd=" + spjangcd);
        }

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
                  AND b.deduct_type = 'EC'
                ORDER BY b.id
                """, param);

        if (billings.isEmpty()) throw new IllegalStateException("PENDING 청구 건 없음");

        // 필수 요소 검증 — 누락 건은 ERROR 처리, 유효 건만 파일에 포함
        List<Long> invalidIds = new java.util.ArrayList<>();
        List<Map<String, Object>> validBillings = new java.util.ArrayList<>();
        for (Map<String, Object> b : billings) {
            if (StringUtils.hasText(str(b.get("bank_code")))
                    && StringUtils.hasText(str(b.get("bank_account")))
                    && b.get("billing_amount") != null) {
                validBillings.add(b);
            } else {
                invalidIds.add(((Number) b.get("id")).longValue());
                log.warn("[CmsEc21] 필수 항목 누락 billing_id={}", b.get("id"));
            }
        }
        if (!invalidIds.isEmpty()) {
            cmsBillingService.updateStatusToError(invalidIds, "필수 항목 누락(은행코드/계좌/예금주/금액)");
            log.warn("[CmsEc21] 필수 항목 누락 ERROR 처리: {}건", invalidIds.size());
        }
        if (validBillings.isEmpty()) throw new IllegalStateException("유효한 PENDING 청구 건 없음");
        billings = validBillings;

        // 2. EC21 생성
        byte[] fileBytes = buildEc21File(spjangcd, billings, institutionCode, targetDate);
        // 파일명: EC21{MMDD}_{YYYY} (금결원 규격)
        String mmdd     = targetDate.substring(4, 8);
        String yyyy     = targetDate.substring(0, 4);
        String fileName = "EC21" + mmdd + "_" + yyyy;
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
                    :spjangcd, :fileName, 'EC_REQUEST', :filePath,
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
            sftpSendWithApiCredential(fileBytes, fileName, targetDate, spjangcd);
            sent = true;
        } catch (Exception e) {
            errMsg = e.getMessage();
            log.error("[CmsEc21FileGenerate] SFTP 전송 실패 spjangcd={}: {}", spjangcd, e.getMessage());
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
            int updatedCount = cmsBillingService.updateStatusToRequested(billingIds, fileId, feeRequest);
            log.info("[CmsEc21FileGenerate] billing REQUESTED 전환: {}건", updatedCount);
        } else {
            cmsBillingService.updateStatusToError(billingIds, errMsg);
            log.warn("[CmsEc21FileGenerate] billing ERROR 전환: {}건", billingIds.size());
            throw new IllegalStateException("SFTP 전송 실패: " + errMsg);
        }

        log.info("[CmsEc21FileGenerate] spjangcd={} 파일={} {}건 {}원 SFTP={}",
                spjangcd, fileName, billings.size(), totalAmount, sent ? "OK" : "FAILED");

        // 수정 - 전송 후 상태 확인 → 오류 시 FAILED 처리
        try {
            JsonNode statusNode = cmsTokenService.getFileStatus(spjangcd, "EC21", targetDate, true);
            int fileStatus = statusNode.path("data").path("file_status").asInt(-1);
            log.info("[CmsEc21] 파일상태확인 spjangcd={} file_status={}", spjangcd, fileStatus);

            if (fileStatus >= 2 && fileStatus <= 4) {
                // 센터 오류 - 상세 조회
                String errDetail = "";
                try {
                    JsonNode errNode = cmsTokenService.getCenterError(spjangcd, "EC21", targetDate);
                    errDetail = errNode.path("data").path("validation_message").asText("");
                } catch (Exception ignored) {}

                String errMsg2 = "금결원 센터오류 (status=" + fileStatus + ")" + (errDetail.isEmpty() ? "" : ": " + errDetail);
                sqlRunner.execute(/* skip_tenant_check */
                        "UPDATE cms_file SET send_status='FAILED', error_message=:msg, _modified=NOW() WHERE id=:id",
                        new MapSqlParameterSource("id", fileId).addValue("msg", errMsg2));
                cmsBillingService.updateStatusToError(billingIds, errMsg2);
                log.warn("[CmsEc21] 센터오류 FAILED 처리 spjangcd={} fileStatus={}", spjangcd, fileStatus);
            }
        } catch (Exception e) {
            log.warn("[CmsEc21] 파일상태 확인 실패 (무시) spjangcd={}: {}", spjangcd, e.getMessage());
        }
        return fileId;
    }

    /** 수동 SFTP 재전송 (CmsFileService에서 호출) */
    public void sftpSendBytes(byte[] fileBytes, String fileName, String targetDate, String spjangcd) throws Exception {
        sftpSendWithApiCredential(fileBytes, fileName, targetDate, spjangcd);
    }

    /** EC21 재시도 — ERROR 건을 PENDING으로 리셋 후 재전송 (스케줄러 호출). 실패한 spjangcd 목록 반환 */
    public List<String> retry(String targetDate) {
        List<Map<String, Object>> spjangs = sqlRunner.getRows(/* skip_tenant_check */
                "SELECT DISTINCT spjangcd FROM cms_billing WHERE deduct_date=:td AND status='ERROR' AND deduct_type='EC'",
                new MapSqlParameterSource("td", targetDate));

        if (spjangs.isEmpty()) {
            log.info("[CmsEc21Retry] ERROR 건 없음 - 재시도 종료");
            return List.of();
        }
        log.info("[CmsEc21Retry] 재시도 시작 - 대상 사업장: {}", spjangs.size());

        List<String> failed = new java.util.ArrayList<>();
        for (Map<String, Object> row : spjangs) {
            String spjangcd = (String) row.get("spjangcd");
            sqlRunner.execute(/* skip_tenant_check */
                    "UPDATE cms_billing SET status='PENDING',result_msg=NULL,_modified=NOW() WHERE spjangcd=:s AND deduct_date=:td AND status='ERROR' AND deduct_type='EC'",
                    new MapSqlParameterSource("s", spjangcd).addValue("td", targetDate));
            try {
                generateAndSend(spjangcd, targetDate);
            } catch (Exception e) {
                log.error("[CmsEc21Retry] 실패 spjangcd={}: {}", spjangcd, e.getMessage(), e);
                failed.add(spjangcd);
            }
        }
        log.info("[CmsEc21Retry] 재시도 완료");
        return failed;
    }

    /** 사용자 수동 재전송 — PENDING, ERROR 건 선택 후 재전송 */
    public Map<String, Object> resendBilling(List<Long> billingIds) {
        List<Map<String, Object>> groups = sqlRunner.getRows(/* skip_tenant_check */
                "SELECT DISTINCT spjangcd, deduct_date FROM cms_billing WHERE id IN (:ids) AND status IN ('PENDING', 'ERROR') AND deduct_type='EC'",
                new MapSqlParameterSource("ids", billingIds));

        int sent = 0, failed = 0;
        for (Map<String, Object> g : groups) {
            String spjangcd  = (String) g.get("spjangcd");
            String targetDate = (String) g.get("deduct_date");
            try {
                // ERROR 건 → PENDING 리셋 후 generateAndSend
                sqlRunner.execute(/* skip_tenant_check */
                        """
                        UPDATE cms_billing SET status='PENDING', result_msg=NULL, _modified=NOW()
                        WHERE spjangcd=:s AND deduct_date=:td AND status='ERROR' AND deduct_type='EC'
                        """,
                        new MapSqlParameterSource("s", spjangcd).addValue("td", targetDate));

                generateAndSend(spjangcd, targetDate);
                log.info("[CmsEc21Resend] 파일 생성+전송: spjangcd={} targetDate={}", spjangcd, targetDate);
                sent++;
            } catch (Exception e) {
                failed++;
                log.error("[CmsEc21Resend] 실패 spjangcd={}: {}", spjangcd, e.getMessage(), e);
            }
        }

        var result = new java.util.HashMap<String, Object>();
        result.put("sent", sent);
        result.put("failed", failed);
        return result;
    }



    // ── SFTP 1회용 계정 획득 + 전송 ───────────────────────────────────────────

    private void sftpSendWithApiCredential(byte[] fileBytes, String fileName, String targetDate, String spjangcd) throws Exception {
        String[] cred = cmsTokenService.getSftpSendCredential(spjangcd, "EC21", targetDate);
        sftpUpload(fileBytes, fileName, cred[0], cred[1]);
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
            log.info("[CmsEc21File] SFTP 전송 시작 파일={} 크기={}bytes", fileName, fileBytes.length);
            channel.put(new java.io.ByteArrayInputStream(fileBytes), fileName, ChannelSftp.OVERWRITE);
        } catch (Exception e) {
            String msg = e.getMessage() != null ? e.getMessage() : "";
            if (msg.contains("End of IO") || msg.contains("inputstream is closed")) {
                log.warn("[CmsEc21File] SFTP 서버 강제종료(정상): {}", msg);
            } else {
                throw new JSchException("SFTP 전송 실패: " + msg, e);
            }
        } finally {
            try { channel.disconnect(); } catch (Exception ignored) {}
            try { session.disconnect(); } catch (Exception ignored) {}
        }
    }

    // ── EC21 파일 포맷 ─────────────────────────────────────────────────────────

    private byte[] buildEc21File(String spjangcd, List<Map<String, Object>> billings,
                                 String institutionCode, String targetDate) throws IOException {
        Map<String, Object> spjang = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT cms_bank_branch, cms_recv_account, cms_description FROM tb_xa012_cms WHERE spjangcd=:s",
                new MapSqlParameterSource("s", spjangcd));

        String fileNameInRecord = "EC21" + targetDate.substring(4, 8);
        String deductDateYY     = targetDate.substring(2, 8);

        long totalAmount = 0;
        var baos = new ByteArrayOutputStream();

        // Header (150 bytes)
        var h = new ByteArrayOutputStream();
        h.write(anBytes("H",            1));
        h.write(anBytes("00000000",     8));
        h.write(anBytes(institutionCode, 10));
        h.write(anBytes(fileNameInRecord, 8));
        h.write(anBytes(deductDateYY,   6));
        h.write(anBytes(spjang != null ? str(spjang.get("cms_bank_branch")) : "", 7));
        h.write(anBytes(spjang != null ? str(spjang.get("cms_recv_account")).replaceAll("-","") : "", 16));
        h.write(spaces(94));
        baos.write(ensure150(h.toByteArray()));

        // Data Records (150 bytes each)
        int seq = 1;
        for (Map<String, Object> b : billings) {
            long amount = b.get("billing_amount") != null ? ((Number) b.get("billing_amount")).longValue() : 0L;
            totalAmount += amount;

            var r = new ByteArrayOutputStream();
            r.write(anBytes("R",                                         1));
            r.write(nBytes(String.valueOf(seq++),                        8));
            r.write(anBytes(institutionCode,                            10));
            r.write(anBytes(str(b.get("bank_code")) + "0000",           7));
            r.write(anBytes(str(b.get("bank_account")).replaceAll("-",""), 16));
            r.write(nBytes(String.valueOf(amount),                      13));
            r.write(anBytes(str(b.get("id_number")),                    13));
            r.write(anBytes(" ",                                         1));
            r.write(anBytes("    ",                                      4));
            r.write(descBytes(spjang != null ? str(spjang.get("cms_description")) : ""));
            r.write(anBytes("  ",                                        2));
            r.write(anBytes(str(b.get("member_no")),                    20));
            r.write(anBytes("     ",                                     5));
            r.write(anBytes("1",                                         1));
            r.write(anBytes(str(b.get("phone")).replaceAll("[^0-9]",""), 12));
            r.write(spaces(21));
            baos.write(ensure150(r.toByteArray()));
        }

        // Trailer (150 bytes)
        var t = new ByteArrayOutputStream();
        t.write(anBytes("T",             1));
        t.write(anBytes("99999999",      8));
        t.write(anBytes(institutionCode, 10));
        t.write(anBytes(fileNameInRecord, 8));
        t.write(nBytes(String.valueOf(seq - 1), 8));
        t.write(nBytes(String.valueOf(seq - 1), 8));
        t.write(nBytes(String.valueOf(totalAmount), 13));
        t.write(nBytes("0",              8));
        t.write(nBytes("0",             13));
        t.write(spaces(63));
        t.write(spaces(10));
        baos.write(ensure150(t.toByteArray()));

        return baos.toByteArray();
    }

    /** AN 타입: 우측 공백 패딩, 지정 바이트 수로 자름 */
    private byte[] anBytes(String s, int len) {
        byte[] src = (s != null ? s : "").getBytes(EUC_KR);
        byte[] result = new byte[len];
        Arrays.fill(result, (byte) ' ');
        System.arraycopy(src, 0, result, 0, Math.min(src.length, len));
        return result;
    }

    /** N 타입: 좌측 0 패딩 */
    private byte[] nBytes(String s, int len) {
        byte[] src = (s != null ? s : "0").getBytes(EUC_KR);
        byte[] result = new byte[len];
        Arrays.fill(result, (byte) '0');
        int offset = len - Math.min(src.length, len);
        System.arraycopy(src, 0, result, offset, Math.min(src.length, len));
        return result;
    }

    /** 통장기재내용: EUC-KR 바이트 기준 16바이트, 우측 공백 패딩 */
    private byte[] descBytes(String desc) {
        byte[] result = new byte[16];
        // 전각 공백(EUC-KR 0xA1A1)으로 초기화
        for (int i = 0; i < 8; i++) {
            result[i * 2]     = (byte) 0xA1;
            result[i * 2 + 1] = (byte) 0xA1;
        }
        if (StringUtils.hasText(desc)) {
            byte[] src = desc.getBytes(EUC_KR);
            System.arraycopy(src, 0, result, 0, Math.min(src.length, 16));
        }
        return result;
    }

    /** 공백 바이트 배열 */
    private byte[] spaces(int len) {
        byte[] b = new byte[len];
        Arrays.fill(b, (byte) ' ');
        return b;
    }

    /** 150바이트 보장 */
    private byte[] ensure150(byte[] b) {
        if (b.length == 150) return b;
        byte[] result = new byte[150];
        Arrays.fill(result, (byte) ' ');
        System.arraycopy(b, 0, result, 0, Math.min(b.length, 150));
        return result;
    }

    private String str(Object v) { return v != null ? v.toString() : ""; }
}
