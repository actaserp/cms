package mes.app.Scheduler.SchedulerService;

import com.fasterxml.jackson.databind.JsonNode;
import com.jcraft.jsch.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mes.app.cms.service.CmsErpResultSyncService;
import mes.app.cms.service.CmsTokenService;
import mes.app.files.NcpObjectStorageService;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * D 23:00 실행 — 금결원 SFTP에서 EC22(불능분) 수신 → billing SUCCESS/FAIL 업데이트
 *
 * EC22 = 당일출금 결과 파일 (불능건만 포함)
 * → EC22에 있으면 FAIL, 없으면 SUCCESS
 *
 * SFTP 연동: GET /biz/batch?file_type=EC22&transaction_date=YYYYMMDD 로 1회용 계정 획득
 * 파일명: EC22{MMDD}_{YYYY} (연도 suffix 필수)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CmsEc22ReceiveService {

    private static final String FEATURE_CODE = "EC22";
    private static final Charset EUC_KR = Charset.forName("EUC-KR");

    private final SqlRunner sqlRunner;
    private final NcpObjectStorageService storageService;
    private final CmsTokenService cmsTokenService;
    private final CmsErpResultSyncService cmsErpResultSyncService;

    @Value("${cms.sftp-host:tsftp.cmsedi.or.kr}")
    private String sftpHost;

    @Value("${cms.sftp-port:11133}")
    private int sftpPort;

    public void run() {
        log.info("[CmsEc22Receive] 시작");

        List<Map<String, Object>> targets = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT f.spjangcd, TO_CHAR(f.target_date, 'YYYYMMDD') AS target_date, f.id AS file_id
                FROM cms_file f
                WHERE f.file_type = 'EC_REQUEST'
                  AND f.send_status = 'SENT'
                  AND NOT EXISTS (
                      SELECT 1 FROM cms_file r
                      WHERE r.spjangcd = f.spjangcd
                        AND r.file_type = 'EC_RESULT'
                        AND r.target_date = f.target_date
                  )
                """,
                new MapSqlParameterSource());

        if (targets.isEmpty()) {
            log.info("[CmsEc22Receive] 미처리 건 없음 - 종료");
            return;
        }

        for (Map<String, Object> row : targets) {
            String spjangcd = str(row.get("spjangcd"));
            String targetDate = str(row.get("target_date"));
            long fileId = ((Number) row.get("file_id")).longValue();
            String mmdd = targetDate.substring(4, 8);
            String yyyy = targetDate.substring(0, 4);
            String ec22FileName = "EC22" + mmdd + "_" + yyyy;
            log.info("[CmsEc22Receive] 처리 시작 spjangcd={} targetDate={}", spjangcd, targetDate);
            try {
                processSpjang(spjangcd, targetDate, ec22FileName, fileId);
            } catch (Exception e) {
                log.error("[CmsEc22Receive] 실패 spjangcd={} targetDate={}: {}", spjangcd, targetDate, e.getMessage(), e);
            }
        }
    }

    // 기존 메서드 유지 (스케줄러용)
    private void processSpjang(String spjangcd, String targetDate, String ec22FileName, long fileId) throws Exception {
        byte[] fileBytes = sftpDownloadWithApiCredential(ec22FileName, targetDate, spjangcd);
        processSpjang(spjangcd, targetDate, ec22FileName, fileId, fileBytes);
    }

    // 새 오버로드 메서드 (수동 수신용 - 이미 다운받은 fileBytes 사용)
    private void processSpjang(String spjangcd, String targetDate, String ec22FileName, long fileId, byte[] fileBytes) throws Exception {
        long resultFileId = -1L;
        try (ByteArrayInputStream bis = new ByteArrayInputStream(fileBytes)) {
            String objectKey = storageService.buildObjectKey(spjangcd, FEATURE_CODE, ec22FileName);
            storageService.upload(objectKey, bis, fileBytes.length, "text/plain");
            Map<String, Object> resultFileRow = sqlRunner.getRow(/* skip_tenant_check */
                    """
                    INSERT INTO cms_file (
                        spjangcd, file_name, file_type, file_path,
                        target_date, billing_count, billing_amount,
                        send_status, _creater_id, _created, _modifier_id, _modified
                    ) VALUES (
                        :spjangcd, :fileName, 'EC_RESULT', :filePath,
                        CAST(:targetDate AS DATE), 0, 0,
                        'RECEIVED', 'SYSTEM', NOW(), 'SYSTEM', NOW()
                    ) RETURNING id
                    """,
                    new MapSqlParameterSource()
                            .addValue("spjangcd",   spjangcd)
                            .addValue("fileName",   ec22FileName)
                            .addValue("filePath",   objectKey)
                            .addValue("targetDate", targetDate));
            if (resultFileRow != null) {
                resultFileId = ((Number) resultFileRow.get("id")).longValue();
            }
        } catch (Exception e) {
            log.warn("[CmsEc22Receive] NCP 업로드 실패 (처리는 계속): {}", e.getMessage());
        }

        Map<String, String> failMap = parseEc22(fileBytes);
        log.info("[CmsEc22Receive] 불능 건수 spjangcd={}: {}", spjangcd, failMap.size());

        List<Map<String, Object>> requestedBillings = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT b.id, b.billing_amount, b.bank_account, b.fee_request,
                  m.member_no, m.member_name, fb.line_seq
                FROM cms_file_billing fb
                JOIN cms_billing b ON b.id = fb.billing_id
                LEFT JOIN cms_member m ON m.id = b.member_id
                WHERE fb.file_id = :fileId
                  AND b.status = 'REQUESTED'
                ORDER BY fb.line_seq
                """,
                new MapSqlParameterSource("fileId", fileId));

        Map<String, Object> cmsInfo = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT ec21_fee_success FROM tb_xa012_cms WHERE spjangcd = :spjangcd",
                new MapSqlParameterSource("spjangcd", spjangcd));
        int feeSuccess = cmsInfo != null && cmsInfo.get("ec21_fee_success") != null
                ? ((Number) cmsInfo.get("ec21_fee_success")).intValue() : 0;

        int successCount = 0, failCount = 0;
        for (Map<String, Object> b : requestedBillings) {
            long   billingId = ((Number) b.get("id")).longValue();
            String memberNo  = str(b.get("member_no"));
            var    p         = new MapSqlParameterSource("billingId", billingId);
            p.addValue("resultDate", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")));

            if (failMap.containsKey(memberNo)) {
                String resultCode = failMap.get(memberNo);
                p.addValue("resultCode", resultCode);
                p.addValue("resultMsg",  resolveFailMsg(resultCode));
                sqlRunner.execute(/* skip_tenant_check */
                        """
                        UPDATE cms_billing SET status='FAIL', result_code=:resultCode,
                            result_msg=:resultMsg, result_date=NULL, _modified=NOW()
                        WHERE id=:billingId AND status='REQUESTED'
                        """, p);
                int feeRequest = b.get("fee_request") != null ? ((Number) b.get("fee_request")).intValue() : 0;
                cmsErpResultSyncService.syncResult(spjangcd, targetDate, memberNo,
                        str(b.get("member_name")), str(b.get("bank_account")),
                        ((Number) b.get("line_seq")).intValue(),
                        false, resultCode, (long) feeRequest);
                failCount++;
            } else {
                int feeRequest = b.get("fee_request") != null ? ((Number) b.get("fee_request")).intValue() : 0;
                p.addValue("feeSuccess", feeSuccess);
                sqlRunner.execute(/* skip_tenant_check */
                        """
                        UPDATE cms_billing SET status='SUCCESS', result_code='0000',
                            result_msg='출금성공', result_date=:resultDate,
                            fee_success=:feeSuccess, _modified=NOW()
                        WHERE id=:billingId AND status='REQUESTED'
                        """, p);
                cmsErpResultSyncService.syncResult(spjangcd, targetDate, memberNo,
                        str(b.get("member_name")), str(b.get("bank_account")),
                        ((Number) b.get("line_seq")).intValue(),
                        true, null, (long)(feeRequest + feeSuccess));
                successCount++;
            }
        }
        log.info("[CmsEc22Receive] 완료 spjangcd={} 성공={}건 실패={}건", spjangcd, successCount, failCount);
    }

    /** EC22 파일이 없을 때 (불능 0건) — 전체 REQUESTED → SUCCESS */
    private void markAllSuccess(String targetDate, String spjangcd) {
        // [FIX 2] result_date를 오늘(처리일)이 아닌 출금대상일(targetDate)로 통일
        var p = new MapSqlParameterSource();
        p.addValue("targetDate", targetDate);
        p.addValue("resultDate", targetDate);
        p.addValue("spjangcd", spjangcd);
        sqlRunner.execute(/* skip_tenant_check */
                """
                UPDATE cms_billing SET status='SUCCESS', result_code='0000',
                    result_msg='출금성공', result_date=:resultDate, _modified=NOW()
                WHERE deduct_date=:targetDate AND status='REQUESTED' AND spjangcd=:spjangcd
                """, p);
    }

    /**
     * EC22 파싱: R레코드에서 납부자번호(20자) + 불능코드(4자) 추출
     * EC22 Data Record (150B):
     *   pos  1     : R
     *   pos  2-9   : 일련번호 (8)
     *   pos 10-19  : 기관코드 (10)
     *   pos 20-26  : 출금은행점코드 (7)
     *   pos 27-42  : 출금계좌번호 (16)
     *   pos 43-55  : 출금의뢰금액 (13)
     *   pos 56-68  : 예금주생년월일 (13)
     *   pos 69-73  : 출금결과 (5: 출금여부1 + 불능코드4)
     *   pos 74-89  : 통장기재내용 (16)
     *   pos 90-91  : 자금종류 (2)
     *   pos 92-111 : 납부자번호 (20)
     */
    private Map<String, String> parseEc22(byte[] fileBytes) {
        Map<String, String> failMap = new LinkedHashMap<>();
        int recordLen = 150;
        for (int i = 0; i + recordLen <= fileBytes.length; i += recordLen) {
            byte[] lineBytes = Arrays.copyOfRange(fileBytes, i, i + recordLen);
            char recType = (char) (lineBytes[0] & 0xFF);
            if (recType != 'R') continue;

            String resultField = new String(Arrays.copyOfRange(lineBytes, 68, 73), EUC_KR).trim();
            String resultCode  = resultField.length() >= 4 ? resultField.substring(1) : resultField;

            String memberNo = new String(Arrays.copyOfRange(lineBytes, 91, 111), EUC_KR).trim();
            if (!memberNo.isEmpty()) {
                failMap.put(memberNo, resultCode);
            }
        }
        return failMap;
    }

    private byte[] sftpDownloadWithApiCredential(String fileName, String targetDate, String spjangcd) throws Exception {
        String[] cred = cmsTokenService.getSftpReceiveCredential(spjangcd, "EC22", targetDate);
        String sftpUser = cred[0];
        String sftpPass = cred[1];

        log.info("[CmsEc22Receive] SFTP 1회용 계정 획득: user={}", sftpUser);

        JSch jsch = new JSch();
        Session session = jsch.getSession(sftpUser, sftpHost, sftpPort);
        session.setPassword(sftpPass);
        java.util.Properties config = new java.util.Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);
        session.connect(15000);

        ChannelSftp channel = (ChannelSftp) session.openChannel("sftp");

        Vector<ChannelSftp.LsEntry> list = channel.ls(".");
        for (ChannelSftp.LsEntry entry : list) {
            log.info("[CmsEc22Receive] SFTP 파일 목록: {}", entry.getFilename());
        }

        log.info("[CmsEc22Receive] SFTP pwd: {}", channel.pwd());
        log.info("[CmsEc22Receive] SFTP 다운로드 시도 파일명: {}", fileName);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            try {
                channel.get(fileName, baos);
            } catch (SftpException e) {
                String msg = e.getMessage() != null ? e.getMessage() : "";
                if (msg.contains("inputstream is closed") || msg.contains("End of IO")) {
                    log.info("[CmsEc22Receive] SFTP 서버 강제종료(정상) - 수신 데이터 확인: {}bytes", baos.size());
                    if (baos.size() > 0) {
                        return baos.toByteArray();
                    }
                }
                throw e;
            }
            return baos.toByteArray();
        } finally {
            try { channel.disconnect(); } catch (Exception ignored) {}
            try { session.disconnect(); } catch (Exception ignored) {}
        }
    }

    private String resolveFailMsg(String code) {
        if (code == null) return "출금실패";
        return switch (code) {
            case "0012" -> "계좌번호 오류 또는 계좌번호 없음";
            case "0014" -> "사업자등록번호 또는 생년월일 오류";
            case "0015" -> "계정과목 오류";
            case "0017" -> "출금이체 미신청계좌";
            case "0018" -> "출금이체신청 임의해지";
            case "0019" -> "출금이체신청 은행 해지";
            case "0020" -> "자동납부 동의자료 부재로 인한 임의해지";
            case "0021" -> "잔액 또는 지불가능잔액 부족";
            case "0022" -> "입금한도 초과";
            case "0024" -> "계좌변경으로 인한 출금이체신청 해지";
            case "0031" -> "해약계좌";
            case "0032" -> "가명계좌 또는 실명미확인";
            case "0033" -> "잡좌";
            case "0034" -> "법적제한계좌, 지급정지 또는 사고신고계좌";
            case "0035" -> "압류·가압류 계좌";
            case "0036" -> "잔액증명발급 계좌";
            case "0037" -> "연체계좌 또는 지점통제계좌";
            case "0038" -> "거래중지계좌";
            case "0041" -> "은행시스템 오류";
            case "0051" -> "기타 오류";
            case "0061" -> "의뢰금액 0원";
            case "0062" -> "건당 이체금액한도 초과";
            case "0065" -> "법인계좌 사용불가";
            case "0066" -> "투자자예탁금이 아님";
            case "0068" -> "통장기재내용 오류";
            case "0075" -> "출금형태 오류 또는 의뢰금액 미달";
            case "0097" -> "농협 은행점코드 오류";
            case "9998" -> "기타 오류";
            case "9999" -> "은행시스템 장애";
            default     -> "출금실패(" + code + ")";
        };
    }

    public List<Map<String, Object>> getAvailableEc22Files(String spjangcd) {
        log.info("[CmsEc22Receive] 파일 목록 조회 시작 spjangcd={}", spjangcd);
        List<Map<String, Object>> result = new ArrayList<>();

        try {
            JsonNode node = cmsTokenService.getFileList(spjangcd, "EC22");
            if (node == null) {
                log.info("[CmsEc22Receive] 파일 목록 없음 spjangcd={}", spjangcd);
                return result;
            }

            JsonNode files = node.path("data").path("content"); // ← data.content 로 수정
            for (JsonNode file : files) {
                String transactionDate = file.path("transaction_date").asText();
                if (!StringUtils.hasText(transactionDate)) continue; // ← 방어 코드 추가

                String yyyy    = transactionDate.substring(0, 4);
                String mmdd    = transactionDate.substring(4, 8);
                String apiFileName = file.path("file_name").asText(); // "EB220515"
                String fileName    = apiFileName + "_" + yyyy;        // "EB220515_2026"

                Map<String, Object> fileInfo = new HashMap<>();
                fileInfo.put("fileName",   fileName);
                fileInfo.put("mmdd",       mmdd);
                fileInfo.put("yyyy",       yyyy);
                fileInfo.put("fileStatus", file.path("file_status").asInt());
                fileInfo.put("processed",  isEc22FileAlreadyProcessed(spjangcd, transactionDate, fileName));

                result.add(fileInfo);
                log.info("[CmsEc22Receive] 발견된 파일: {} (처리됨: {})", fileName, fileInfo.get("processed"));
            }

            result.sort((a, b) -> ((String)b.get("fileName")).compareTo((String)a.get("fileName")));
            log.info("[CmsEc22Receive] 파일 목록 조회 완료: {}건", result.size());

        } catch (Exception e) {
            log.error("[CmsEc22Receive] 파일 목록 조회 실패: {}", e.getMessage(), e);
            throw new RuntimeException("파일 목록 조회 실패: " + e.getMessage(), e);
        }

        return result;
    }

    private boolean isEc22FileAlreadyProcessed(String spjangcd, String targetDate, String fileName) {
        try {
            Map<String, Object> row = sqlRunner.getRow(/* skip_tenant_check */
                    """
                    SELECT COUNT(*) as cnt FROM cms_file
                    WHERE spjangcd = :spjangcd
                      AND file_name = :fileName
                      AND file_type = 'EC_RESULT'
                      AND target_date = CAST(:targetDate AS DATE)
                    """,
                    new MapSqlParameterSource()
                            .addValue("spjangcd", spjangcd)
                            .addValue("fileName", fileName)
                            .addValue("targetDate", targetDate));

            long count = row != null ? ((Number) row.get("cnt")).longValue() : 0L;
            return count > 0;
        } catch (Exception e) {
            log.warn("[CmsEc22Receive] 파일 처리 여부 확인 실패: {}", e.getMessage());
            return false;
        }
    }

    public Map<String, Object> processSelectedEc22File(String spjangcd, String fileName) {
        log.info("[CmsEc22Receive] 파일 수동 처리 시작 spjangcd={}, fileName={}", spjangcd, fileName);

        Map<String, Object> result = new HashMap<>();
        result.put("success", false);
        result.put("message", "");

        try {
            // 파일명 형식 검증
            if (!fileName.startsWith("EC22")) {
                throw new IllegalArgumentException("유효하지 않은 파일명: " + fileName);
            }

            String[] parts = fileName.replace("EC22", "").split("_");
            if (parts.length != 2 || parts[0].length() != 4 || parts[1].length() != 4) {
                throw new IllegalArgumentException("파일명 형식 오류: " + fileName);
            }

            String mmdd = parts[0];
            String yyyy = parts[1];
            String targetDate = yyyy + mmdd;

            // 파일이 이미 처리됐는지 확인
            if (isEc22FileAlreadyProcessed(spjangcd, targetDate, fileName)) {
                log.warn("[CmsEc22Receive] 이미 처리된 파일: {}", fileName);
                result.put("success", true);
                result.put("message", "이미 처리된 파일입니다.");
                return result;
            }

            // SFTP에서 파일 다운로드 후 처리 (기존 processSpjang 로직 사용)
            long fileId = -1L;
            byte[] fileBytes = sftpDownloadWithApiCredential(fileName, targetDate, spjangcd);

            // processSpjang과 동일한 로직 수행
            processSpjang(spjangcd, targetDate, fileName, fileId, fileBytes);

            result.put("success", true);
            result.put("message", "파일 처리가 완료되었습니다.");
            log.info("[CmsEc22Receive] 파일 수동 처리 완료: {}", fileName);

        } catch (SftpException e) {
            log.info("[CmsEc22Receive] EC22 파일 없음: {}", e.getMessage());
            result.put("message", "SFTP에서 파일을 찾을 수 없습니다.");
        } catch (Exception e) {
            log.error("[CmsEc22Receive] 파일 처리 실패: {}", e.getMessage(), e);
            result.put("message", "처리 실패: " + e.getMessage());
        }

        return result;
    }

    private String str(Object v) { return v != null ? v.toString() : ""; }
}