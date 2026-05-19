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
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * D+1 04:00 실행 — 금결원 SFTP에서 EB22(불능분) 수신 → billing SUCCESS/FAIL 업데이트
 *
 * EB22 = 출금결과 파일 (불능건만 포함)
 * → EB22에 있으면 FAIL, 없으면 SUCCESS
 *
 * SFTP 연동: GET /biz/batch?file_type=EB22&transaction_date=YYYYMMDD 로 1회용 계정 획득
 * 파일명: EB22{MMDD}_{YYYY} (연도 suffix 필수)
 *
 * 트랜잭션 전략:
 * - PostgreSQL: @Transactional로 묶어 중간 실패 시 롤백
 * - MSSQL: PostgreSQL 커밋 완료 후 일괄 처리 (CmsErpResultSyncService에서 별도 트랜잭션)
 * - PostgreSQL 실패 시 MSSQL INSERT 자체를 실행하지 않아 정합성 보장
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CmsEb22ReceiveService {

    private static final String FEATURE_CODE = "EB22";
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
        log.info("[CmsEb22Receive] 시작");

        List<Map<String, Object>> targets = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT f.spjangcd, TO_CHAR(f.target_date, 'YYYYMMDD') AS target_date, f.id AS file_id
                FROM cms_file f
                WHERE f.file_type = 'EB_REQUEST'
                  AND f.send_status = 'SENT'
                  AND NOT EXISTS (
                      SELECT 1 FROM cms_file r
                      WHERE r.spjangcd = f.spjangcd
                        AND r.file_type = 'EB_RESULT'
                        AND r.target_date = f.target_date
                  )
                """,
                new MapSqlParameterSource());

        if (targets.isEmpty()) {
            log.info("[CmsEb22Receive] 미처리 건 없음 - 종료");
            return;
        }

        for (Map<String, Object> row : targets) {
            String spjangcd   = str(row.get("spjangcd"));
            String targetDate = str(row.get("target_date"));
            long   fileId     = ((Number) row.get("file_id")).longValue();
            String mmdd       = targetDate.substring(4, 8);
            String yyyy       = targetDate.substring(0, 4);
            String eb22FileName = "EB22" + mmdd + "_" + yyyy;
            log.info("[CmsEb22Receive] 처리 시작 spjangcd={} targetDate={}", spjangcd, targetDate);
            try {
                byte[] fileBytes = sftpDownloadWithApiCredential(eb22FileName, targetDate, spjangcd);
                processSpjangInternal(spjangcd, targetDate, eb22FileName, fileId, fileBytes);
            } catch (Exception e) {
                log.error("[CmsEb22Receive] 실패 spjangcd={} targetDate={}: {}", spjangcd, targetDate, e.getMessage(), e);
            }
        }
    }

    /**
     * 실제 처리 로직
     *
     * @Transactional로 PostgreSQL 처리 전체를 하나의 트랜잭션으로 묶음
     * - cms_file INSERT, cms_billing UPDATE 모두 성공해야 커밋
     * - 중간 실패 시 전체 롤백
     *
     * MSSQL INSERT는 PostgreSQL 커밋 이후에 호출 (syncResults)
     * - PostgreSQL이 정상 커밋된 경우에만 MSSQL에 INSERT
     * - MSSQL 실패는 로그만 남기고 PostgreSQL 롤백은 하지 않음 (이미 커밋됨)
     *
     * @Transactional이 동작하려면 Spring 프록시를 통해 호출되어야 하므로 public
     */
    @Transactional
    public void processSpjangInternal(String spjangcd, String targetDate, String eb22FileName, long fileId, byte[] fileBytes) throws Exception {

        // NCP 업로드 (실패해도 처리는 계속 - 파일은 SFTP에 있으므로 재업로드 가능)
        String objectKey = storageService.buildObjectKey(spjangcd, FEATURE_CODE, eb22FileName);
        try (ByteArrayInputStream bis = new ByteArrayInputStream(fileBytes)) {
            storageService.upload(objectKey, bis, fileBytes.length, "text/plain");
        } catch (Exception e) {
            log.warn("[CmsEb22Receive] NCP 업로드 실패 (처리는 계속): {}", e.getMessage());
        }

        // cms_file(EB_RESULT) INSERT
        long resultFileId = -1L;
        Map<String, Object> resultFileRow = sqlRunner.getRow(/* skip_tenant_check */
                """
                INSERT INTO cms_file (
                    spjangcd, file_name, file_type, file_path,
                    target_date, billing_count, billing_amount,
                    send_status, _creater_id, _created, _modifier_id, _modified, send_type
                ) VALUES (
                    :spjangcd, :fileName, 'EB_RESULT', :filePath,
                    CAST(:targetDate AS DATE), 0, 0,
                    'RECEIVED', 'SYSTEM', NOW(), 'SYSTEM', NOW(), 'SFTP'
                ) RETURNING id
                """,
                new MapSqlParameterSource()
                        .addValue("spjangcd",   spjangcd)
                        .addValue("fileName",   eb22FileName)
                        .addValue("filePath",   objectKey)
                        .addValue("targetDate", targetDate));
        if (resultFileRow != null) {
            resultFileId = ((Number) resultFileRow.get("id")).longValue();
        }

        // EB22 파싱 (불능건만 추출)
        Map<String, String> failMap = parseEb22(fileBytes);
        log.info("[CmsEb22Receive] 불능 건수 spjangcd={}: {}", spjangcd, failMap.size());

        // 청구 목록 조회
        List<Map<String, Object>> requestedBillings = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT b.id, b.billing_amount, b.bank_account, b.fee_request,
                  m.member_no, m.member_name, fb.line_seq, m.cltcd
                FROM cms_file_billing fb
                JOIN cms_billing b ON b.id = fb.billing_id
                LEFT JOIN cms_member m ON m.id = b.member_id
                WHERE fb.file_id = :fileId
                  AND b.status = 'REQUESTED'
                ORDER BY fb.line_seq
                """,
                new MapSqlParameterSource("fileId", fileId));

        Map<String, Object> cmsInfo = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT eb21_fee_success FROM tb_xa012_cms WHERE spjangcd = :spjangcd",
                new MapSqlParameterSource("spjangcd", spjangcd));
        int feeSuccess = cmsInfo != null && cmsInfo.get("eb21_fee_success") != null
                ? ((Number) cmsInfo.get("eb21_fee_success")).intValue() : 0;

        // MSSQL 동기화 항목 수집 (PostgreSQL 커밋 후 일괄 처리)
        List<CmsErpResultSyncService.SyncItem> syncItems = new ArrayList<>();

        // ✨ 배치 처리: 성공/실패 건을 분리해서 모음
        List<Long> successIds = new ArrayList<>();
        List<Map<String, Object>> failBillings = new ArrayList<>();
        int  successCount = 0, failCount = 0;
        long totalAmount  = 0;
        String resultDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        // 1단계: 성공/실패 분류
        for (Map<String, Object> b : requestedBillings) {
            long   billingId     = ((Number) b.get("id")).longValue();
            long   billingAmount = b.get("billing_amount") != null ? ((Number) b.get("billing_amount")).longValue() : 0;
            int    feeRequest    = b.get("fee_request")    != null ? ((Number) b.get("fee_request")).intValue()    : 0;
            String memberNo      = str(b.get("member_no"));
            String memberName    = str(b.get("member_name"));
            String bankAccount   = str(b.get("bank_account"));
            int    lineSeq       = ((Number) b.get("line_seq")).intValue();
            String cltcd = str(b.get("cltcd"));

            if (failMap.containsKey(memberNo)) {
                String resultCode = failMap.get(memberNo);
                failBillings.add(new java.util.HashMap<String, Object>() {{
                    put("billingId", billingId);
                    put("resultCode", resultCode);
                    put("resultMsg", resolveFailMsg(resultCode));
                    put("memberNo", memberNo);
                    put("memberName", memberName);
                    put("bankAccount", bankAccount);
                    put("lineSeq", lineSeq);
                    put("feeRequest", feeRequest);
                    put("cltcd", cltcd);
                }});
                failCount++;
            } else {
                successIds.add(billingId);
                syncItems.add(new CmsErpResultSyncService.SyncItem(
                        memberNo, memberName, bankAccount, lineSeq,
                        true, null, billingAmount, (long)(feeRequest + feeSuccess), cltcd));
                totalAmount += billingAmount;
                successCount++;
            }
        }

        // 2단계: 배치 UPDATE (성공) - 1회 쿼리
        if (!successIds.isEmpty()) {
            sqlRunner.execute(/* skip_tenant_check */
                    """
                    UPDATE cms_billing SET status='SUCCESS', result_code='0000',
                        result_msg='출금성공', result_date=:resultDate,
                        fee_success=:feeSuccess, _modified=NOW()
                    WHERE id = ANY(:ids::BIGINT[]) AND status='REQUESTED'
                    """,
                    new MapSqlParameterSource()
                            .addValue("ids", successIds.toArray(new Long[0]))
                            .addValue("resultDate", resultDate)
                            .addValue("feeSuccess", feeSuccess));
        }

        // 3단계: 배치 UPDATE (실패) - 1회 쿼리 (결과코드별로 묶을 수도 있지만, 단순화)
        if (!failBillings.isEmpty()) {
            // 결과코드별로 그룹화해서 배치 UPDATE
            Map<String, List<Long>> failIdsByCode = new java.util.HashMap<>();
            List<String> failResultCodes = new ArrayList<>();
            List<String> failResultMsgs = new ArrayList<>();
            List<Long> allFailIds = new ArrayList<>();

            for (Map<String, Object> fail : failBillings) {
                String resultCode = (String) fail.get("resultCode");
                Long billingId = (Long) fail.get("billingId");

                failIdsByCode.computeIfAbsent(resultCode, k -> new ArrayList<>()).add(billingId);
                allFailIds.add(billingId);
            }

            // 각 결과코드별로 배치 UPDATE
            for (Map.Entry<String, List<Long>> entry : failIdsByCode.entrySet()) {
                String resultCode = entry.getKey();
                List<Long> ids = entry.getValue();

                sqlRunner.execute(/* skip_tenant_check */
                        """
                        UPDATE cms_billing SET status='FAIL', result_code=:resultCode,
                            result_msg=:resultMsg, result_date=NULL, _modified=NOW()
                        WHERE id = ANY(:ids::BIGINT[]) AND status='REQUESTED'
                        """,
                        new MapSqlParameterSource()
                                .addValue("ids", ids.toArray(new Long[0]))
                                .addValue("resultCode", resultCode)
                                .addValue("resultMsg", resolveFailMsg(resultCode)));
            }

            // SyncItem 추가 (실패)
            for (Map<String, Object> fail : failBillings) {
                String memberNo = (String) fail.get("memberNo");
                String memberName = (String) fail.get("memberName");
                String bankAccount = (String) fail.get("bankAccount");
                int lineSeq = (int) fail.get("lineSeq");
                String resultCode = (String) fail.get("resultCode");
                int feeRequest = (int) fail.get("feeRequest");
                String cltcd = (String) fail.get("cltcd");

                syncItems.add(new CmsErpResultSyncService.SyncItem(
                        memberNo, memberName, bankAccount, lineSeq,
                        false, resultCode, 0L, (long) feeRequest, cltcd));
            }
        }

        log.info("[CmsEb22Receive] 배치 UPDATE 완료 - 성공={}건, 실패={}건", successCount, failCount);

        // billing_count, billing_amount 업데이트
        if (resultFileId > 0) {
            sqlRunner.execute(/* skip_tenant_check */
                    """
                    UPDATE cms_file SET
                        billing_count  = :count,
                        billing_amount = :amount,
                        _modified      = NOW()
                    WHERE id = :fileId
                    """,
                    new MapSqlParameterSource()
                            .addValue("count",  successCount + failCount)
                            .addValue("amount", totalAmount)
                            .addValue("fileId", resultFileId));
        }

        log.info("[CmsEb22Receive] 완료 spjangcd={} 성공={}건 실패={}건", spjangcd, successCount, failCount);

        // PostgreSQL 트랜잭션 커밋 후 MSSQL INSERT 실행
        // - @Transactional 메서드가 정상 종료돼야 커밋되므로
        // - MSSQL 실패는 로그만 남김 (PostgreSQL은 이미 커밋)
        Map<String, Object> erpInfo = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT host FROM tb_xa012_erp WHERE spjangcd = :spjangcd AND use_yn = 'Y'",
                new MapSqlParameterSource("spjangcd", spjangcd));

        if (erpInfo != null && erpInfo.get("host") != null) {
            // ERP 설정이 있으면 MSSQL에 INSERT
            try {
                cmsErpResultSyncService.syncResults(spjangcd, targetDate, syncItems);
                log.info("[CmsEb22Receive] MSSQL 동기화 완료 spjangcd={} targetDate={}", spjangcd, targetDate);
            } catch (Exception e) {
                log.warn("[CmsEb22Receive] MSSQL 동기화 실패 (PostgreSQL은 정상 처리됨): {}", e.getMessage());
            }
        } else {
            log.info("[CmsEb22Receive] ERP 미설정 - MSSQL 동기화 스킵 spjangcd={} targetDate={}", spjangcd, targetDate);
        }
    }

    /**
     * EB22 파싱: R레코드에서 납부자번호(20자) + 불능코드(4자) 추출
     * EB22 Data Record (150B):
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
    private Map<String, String> parseEb22(byte[] fileBytes) {
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
        String[] cred = cmsTokenService.getSftpReceiveCredential(spjangcd, "EB22", targetDate);
        String sftpUser = cred[0];
        String sftpPass = cred[1];

        log.info("[CmsEb22Receive] SFTP 1회용 계정 획득: user={}", sftpUser);

        JSch jsch = new JSch();
        Session session = jsch.getSession(sftpUser, sftpHost, sftpPort);
        session.setPassword(sftpPass);
        java.util.Properties config = new java.util.Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);
        session.connect(15000);

        ChannelSftp channel = (ChannelSftp) session.openChannel("sftp");
        channel.connect(10000);

        Vector<ChannelSftp.LsEntry> list = channel.ls(".");

        log.info("[CmsEb22Receive] SFTP pwd: {}", channel.pwd());
        log.info("[CmsEb22Receive] SFTP 다운로드 시도 파일명: {}", fileName);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            try {
                channel.get(fileName, baos);
            } catch (SftpException e) {
                String msg = e.getMessage() != null ? e.getMessage() : "";
                if (msg.contains("inputstream is closed") || msg.contains("End of IO")) {
                    log.info("[CmsEb22Receive] SFTP 서버 강제종료(정상) - 수신 데이터 확인: {}bytes", baos.size());
                    if (baos.size() > 0) {
                        log.info("[CmsEb22Receive] 파일 내용:\n{}", new String(baos.toByteArray(), EUC_KR));
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

    /**
     * SFTP에서 사용 가능한 EB22 파일 목록 조회
     */
    public List<Map<String, Object>> getAvailableEb22Files(String spjangcd) {
        log.info("[CmsEb22Receive] 파일 목록 조회 시작 spjangcd={}", spjangcd);
        List<Map<String, Object>> result = new ArrayList<>();

        try {
            JsonNode node = cmsTokenService.getFileList(spjangcd, "EB22");
            if (node == null) {
                log.info("[CmsEb22Receive] 파일 목록 없음 spjangcd={}", spjangcd);
                return result;
            }

            JsonNode files = node.path("data").path("content");
            for (JsonNode file : files) {
                String transactionDate = file.path("transaction_date").asText();
                if (!StringUtils.hasText(transactionDate)) continue;

                String yyyy        = transactionDate.substring(0, 4);
                String mmdd        = transactionDate.substring(4, 8);
                String apiFileName = file.path("file_name").asText();
                String fileName    = apiFileName + "_" + yyyy;

                Map<String, Object> fileInfo = new HashMap<>();
                fileInfo.put("fileName",   fileName);
                fileInfo.put("mmdd",       mmdd);
                fileInfo.put("yyyy",       yyyy);
                fileInfo.put("fileStatus", file.path("file_status").asInt());
                fileInfo.put("processed",  isEb22FileAlreadyProcessed(spjangcd, transactionDate, fileName));

                result.add(fileInfo);
            }

            result.sort((a, b) -> ((String)b.get("fileName")).compareTo((String)a.get("fileName")));
            log.info("[CmsEb22Receive] 파일 목록 조회 완료: {}건", result.size());

        } catch (Exception e) {
            log.error("[CmsEb22Receive] 파일 목록 조회 실패: {}", e.getMessage(), e);
            throw new RuntimeException("파일 목록 조회 실패: " + e.getMessage(), e);
        }

        return result;
    }

    /**
     * EB22 파일이 이미 처리됐는지 확인
     */
    private boolean isEb22FileAlreadyProcessed(String spjangcd, String targetDate, String fileName) {
        try {
            Map<String, Object> row = sqlRunner.getRow(/* skip_tenant_check */
                    """
                    SELECT COUNT(*) as cnt FROM cms_file
                    WHERE spjangcd = :spjangcd
                      AND file_name = :fileName
                      AND file_type = 'EB_RESULT'
                      AND target_date = CAST(:targetDate AS DATE)
                    """,
                    new MapSqlParameterSource()
                            .addValue("spjangcd",   spjangcd)
                            .addValue("fileName",   fileName)
                            .addValue("targetDate", targetDate));

            long count = row != null ? ((Number) row.get("cnt")).longValue() : 0L;
            return count > 0;
        } catch (Exception e) {
            log.warn("[CmsEb22Receive] 파일 처리 여부 확인 실패: {}", e.getMessage());
            return false;
        }
    }

    /**
     * 사용자가 선택한 EB22 파일 수동 처리
     */
    public Map<String, Object> processSelectedEb22File(String spjangcd, String fileName) {
        log.info("[CmsEb22Receive] 파일 수동 처리 시작 spjangcd={}, fileName={}", spjangcd, fileName);

        Map<String, Object> result = new HashMap<>();
        result.put("success", false);
        result.put("message", "");

        try {
            if (!fileName.startsWith("EB22")) {
                throw new IllegalArgumentException("유효하지 않은 파일명: " + fileName);
            }

            String[] parts = fileName.replace("EB22", "").split("_");
            if (parts.length != 2 || parts[0].length() != 4 || parts[1].length() != 4) {
                throw new IllegalArgumentException("파일명 형식 오류: " + fileName);
            }

            String mmdd       = parts[0];
            String yyyy       = parts[1];
            String targetDate = yyyy + mmdd;

            if (isEb22FileAlreadyProcessed(spjangcd, targetDate, fileName)) {
                log.warn("[CmsEb22Receive] 이미 처리된 파일: {}", fileName);
                result.put("success", true);
                result.put("message", "이미 처리된 파일입니다.");
                return result;
            }

            Map<String, Object> requestFileRow = sqlRunner.getRow(/* skip_tenant_check */
                    """
                    SELECT id FROM cms_file
                    WHERE spjangcd = :spjangcd
                      AND file_type = 'EB_REQUEST'
                      AND target_date = CAST(:targetDate AS DATE)
                      AND send_status = 'SENT'
                    """,
                    new MapSqlParameterSource()
                            .addValue("spjangcd",   spjangcd)
                            .addValue("targetDate", targetDate));

            if (requestFileRow == null) {
                result.put("message", "해당 날짜의 청구 파일을 찾을 수 없습니다.");
                return result;
            }

            long   fileId    = ((Number) requestFileRow.get("id")).longValue();
            byte[] fileBytes = sftpDownloadWithApiCredential(fileName, targetDate, spjangcd);
            processSpjangInternal(spjangcd, targetDate, fileName, fileId, fileBytes);

            result.put("success", true);
            result.put("message", "파일 처리가 완료되었습니다.");
            log.info("[CmsEb22Receive] 파일 수동 처리 완료: {}", fileName);

        } catch (SftpException e) {
            log.info("[CmsEb22Receive] EB22 파일 없음: {}", e.getMessage());
            result.put("message", "SFTP에서 파일을 찾을 수 없습니다.");
        } catch (Exception e) {
            log.error("[CmsEb22Receive] 파일 처리 실패: {}", e.getMessage(), e);
            result.put("message", "처리 실패: " + e.getMessage());
        }

        return result;
    }

    private String str(Object v) { return v != null ? v.toString() : ""; }
}