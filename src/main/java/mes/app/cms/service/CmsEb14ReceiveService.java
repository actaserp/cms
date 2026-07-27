package mes.app.cms.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.jcraft.jsch.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mes.app.Scheduler.SchedulerService.CmsEb21SendService;
import mes.app.common.TenantContext;
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
import java.util.*;

/**
 * EB14 — 출금이체 신청결과 수신
 * 불능분만 전송됨 (정상 건은 파일에 없음)
 *
 * ※ 2026-07-23 수정 — 매칭 기준을 apply_date → eb13_sent_at 으로 통일.
 *   기존에는 대상 날짜를 eb13_sent_at 으로 뽑아놓고 UPDATE 는 apply_date 로 비교해서,
 *   동의서 접수일(apply_date)과 전송일이 다른 건이 영구 PENDING 으로 남았다.
 *   (예: 20260714 접수분을 20260721 에 전송 → EB14 0721 처리 대상에서 누락,
 *    eb14_received_at 이 NULL 로 남아 같은 파일을 매 스케줄마다 재다운로드하며
 *    cms_file 행이 중복 적재되는 부작용까지 발생)
 *   EB13 파일의 신청일자 항목에도 전송일자가 찍혀 나가므로 수신 매칭 기준은 eb13_sent_at 이 맞다.
 *
 *   권장 인덱스:
 *     CREATE INDEX idx_car_sentdate ON cms_account_register
 *       (spjangcd, (TO_CHAR(eb13_sent_at,'YYYYMMDD')));
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CmsEb14ReceiveService {

    private static final String FEATURE_CODE = "EB14";
    private static final Charset EUC_KR = Charset.forName("EUC-KR");

    /** 매칭 기준 컬럼식 — apply_date 가 아니라 EB13 실제 전송일자 */
    private static final String SENT_DATE_EXPR = "TO_CHAR(eb13_sent_at, 'YYYYMMDD')";

    private final SqlRunner sqlRunner;
    private final CmsTokenService cmsTokenService;
    private final NcpObjectStorageService storageService;

    @Value("${cms.sftp-host}")
    private String sftpHost;

    @Value("${cms.sftp-port}")
    private int sftpPort;

    public void run() {
        // ★ 기존 MIN(eb13_sent_at) 은 사업장당 날짜를 하나만 뽑아서, 미처리 전송일이 여러 개면
        //   가장 오래된 것만 반복 처리되고 나머지는 계속 밀렸다. DISTINCT 로 전부 순회한다.
        List<Map<String, Object>> targets = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT DISTINCT spjangcd,
                       TO_CHAR(eb13_sent_at, 'YYYYMMDD') AS eb13_sent_date
                FROM cms_account_register
                WHERE eb13_status = 'SENT'
                  AND eb14_received_at IS NULL
                  AND eb13_sent_at >= NOW() - INTERVAL '7 days'
                ORDER BY spjangcd, eb13_sent_date
                """,
                new MapSqlParameterSource());

        for (Map<String, Object> row : targets) {
            String spjangcd     = (String) row.get("spjangcd");
            String eb13SentDate = (String) row.get("eb13_sent_date");
            try {
                receive(spjangcd, eb13SentDate);
            } catch (Exception e) {
                log.error("[CmsEb14] 수신 실패 spjangcd={} date={}: {}",
                        spjangcd, eb13SentDate, e.getMessage(), e);
            }
        }
    }


    public List<Map<String, Object>> getFileList(String spjangcd) throws Exception {
        JsonNode node = cmsTokenService.getFileList(spjangcd, "EB14");
        List<Map<String, Object>> result = new ArrayList<>();
        if (node == null) return result;

        // cms_account_register에서 미처리 eb13_sent_at 날짜 목록 조회
        List<Map<String, Object>> pendingDates = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT DISTINCT TO_CHAR(eb13_sent_at, 'YYYYMMDD') AS sent_date
                FROM cms_account_register
                WHERE spjangcd = :spjangcd
                  AND eb13_status = 'SENT'
                  AND eb14_received_at IS NULL
                """,
                new MapSqlParameterSource("spjangcd", spjangcd));

        Set<String> pendingDateSet = pendingDates.stream()
                .map(r -> (String) r.get("sent_date"))
                .collect(java.util.stream.Collectors.toSet());

        JsonNode files = node.path("data").path("content");
        for (JsonNode file : files) {
            String transactionDate = file.path("transaction_date").asText();
            if (!StringUtils.hasText(transactionDate)) continue;
            // 미처리 날짜에 해당하는 파일만 포함
            if (!pendingDateSet.contains(transactionDate)) continue;

            String mmdd    = transactionDate.substring(4, 8);
            String yyyy     = transactionDate.substring(0, 4);
            String fileName = "EB14" + mmdd + "_" + yyyy;

            Map<String, Object> fileInfo = new HashMap<>();
            fileInfo.put("fileName",        fileName);
            fileInfo.put("transactionDate", transactionDate);
            fileInfo.put("fileStatus",      file.path("file_status").asInt());
            fileInfo.put("processed",       false);
            result.add(fileInfo);
        }

        result.sort((a, b) -> ((String) b.get("transactionDate")).compareTo((String) a.get("transactionDate")));
        return result;
    }

    public void receive(String spjangcd, String targetDate) throws Exception {
        if (!StringUtils.hasText(targetDate)) {
            targetDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        }
        String mmdd     = targetDate.substring(4, 8);
        String yyyy     = targetDate.substring(0, 4);
        String fileName = "EB14" + mmdd + "_" + yyyy;

        String[] cred = cmsTokenService.getSftpReceiveCredential(spjangcd, "EB14", targetDate);
        byte[] fileBytes = sftpDownload(fileName, cred[0], cred[1]);

        if (fileBytes == null) {
            log.info("[CmsEb14] 파일 없음 spjangcd={} file={}", spjangcd, fileName);
            return;
        }

        // NCP 업로드
        String objectKey = storageService.buildObjectKey(spjangcd, "EB14", fileName);
        try (var bis = new ByteArrayInputStream(fileBytes)) {
            storageService.upload(objectKey, bis, fileBytes.length, "application/octet-stream");
        }

        // cms_file INSERT — 같은 (사업장, 파일종류, 일자) 가 이미 있으면 재사용해 중복 적재 방지
        var fp = new MapSqlParameterSource();
        fp.addValue("spjangcd",  spjangcd);
        fp.addValue("fileName",  fileName);
        fp.addValue("filePath",  objectKey);
        fp.addValue("targetDate", targetDate);

        Map<String, Object> fileRow = sqlRunner.getRow(/* skip_tenant_check */
                """
                SELECT id FROM cms_file
                WHERE spjangcd = :spjangcd
                  AND file_type = 'EB14'
                  AND target_date = CAST(:targetDate AS DATE)
                ORDER BY id DESC
                LIMIT 1
                """, fp);

        if (fileRow == null || fileRow.get("id") == null) {
            fileRow = sqlRunner.getRow(/* skip_tenant_check */
                    """
                    INSERT INTO cms_file (
                        spjangcd, file_name, file_type, file_path,
                        target_date, billing_count, billing_amount,
                        send_status, _creater_id, _created, _modifier_id, _modified
                    ) VALUES (
                        :spjangcd, :fileName, 'EB14', :filePath,
                        CAST(:targetDate AS DATE), 0, 0,
                        'RECEIVED', 'SYSTEM', NOW(), 'SYSTEM', NOW()
                    ) RETURNING id
                    """, fp);
        } else {
            sqlRunner.execute(/* skip_tenant_check */
                    """
                    UPDATE cms_file
                    SET file_path = :filePath, _modifier_id = 'SYSTEM', _modified = NOW()
                    WHERE id = :id
                    """,
                    new MapSqlParameterSource("filePath", objectKey)
                            .addValue("id", ((Number) fileRow.get("id")).longValue()));
        }
        long fileId = ((Number) fileRow.get("id")).longValue();

        parseAndUpdate(spjangcd, fileBytes, targetDate, fileId);
    }

    private void parseAndUpdate(String spjangcd, byte[] fileBytes, String targetDate, long fileId) {
        int recordSize   = 120;
        int totalRecords = fileBytes.length / recordSize;

        // ★ 불능 키 = 납부자번호 + 신청구분.
        //   EB14 레코드에 신청구분(pos=25)이 있으므로 금결원은 신규/해지 결과를 "건별"로 준다.
        //   계좌변경은 같은 납부자번호에 신규('1')+해지('3')가 함께 나가므로,
        //   memberNo 만 키로 쓰면 뒤 레코드가 앞 레코드를 덮어써서 한쪽 결과가 소실된다.
        //   (예: 해지 승인 + 신규 불능 → 구계좌가 죽었는데 시스템은 '변경 거절'로 오인 → 다음달 0017 전멸)
        //   2026-07-15 수정.
        Map<String, String> failCodeMap = new LinkedHashMap<>();   // "memberNo|applyType" → failCode

        for (int i = 0; i < totalRecords; i++) {
            byte[] record = Arrays.copyOfRange(fileBytes, i * recordSize, (i + 1) * recordSize);
            String line = new String(record, EUC_KR);
            if (!"R".equals(line.substring(0, 1))) continue;

            // 신청구분(pos=25,len=1) 납부자번호(pos=26,len=20)
            // 처리결과코드(pos=91,len=1) 불능코드(pos=92,len=4)
            String applyType = line.substring(25, 26).trim();
            String memberNo  = line.substring(26, 46).trim();
            String resultCd  = line.substring(91, 92).trim();
            String failCode  = line.substring(92, 96).trim();

            if (!StringUtils.hasText(applyType)) applyType = "1";   // 구파일 방어

            if ("N".equals(resultCd)) {
                failCodeMap.put(memberNo + "|" + applyType, failCode);
                log.info("[CmsEb14] 불능 memberNo={} applyType={} code={}", memberNo, applyType, failCode);
            }
        }

        // ── 불능 건 처리 (건별) ──────────────────────────────────────────────
        for (Map.Entry<String, String> e : failCodeMap.entrySet()) {
            String[] parts   = e.getKey().split("\\|", 2);
            String memberNo  = parts[0];
            String applyType = parts[1];
            String failCode  = e.getValue();

            var p = new MapSqlParameterSource();
            p.addValue("spjangcd",   spjangcd);
            p.addValue("memberNo",   memberNo);
            p.addValue("applyType",  applyType);
            p.addValue("failCode",   failCode);
            p.addValue("targetDate", targetDate);

            // ★ apply_type 조건 필수. 없으면 같은 납부자의 반대편 건(계좌변경 세트)까지 REJECTED 로 오염됨.
            sqlRunner.execute(/* skip_tenant_check */
                    "UPDATE cms_account_register " +
                            "SET eb14_result='N', eb14_fail_code=:failCode, " +
                            "    eb14_received_at=NOW(), status='REJECTED', _modified=NOW() " +
                            "WHERE spjangcd=:spjangcd AND member_no=:memberNo " +
                            "  AND " + SENT_DATE_EXPR + " = :targetDate " +
                            "  AND COALESCE(apply_type,'1') = :applyType",
                    p);

            // 해지 불능 → cms_member 원복 (PENDING_CANCEL → ACTIVE)
            // ※ 계좌변경(change_flag='Y')의 해지행은 제외 — 그건 status 를 건드리지 않았으므로 원복 대상이 아님.
            if ("3".equals(applyType)) {
                sqlRunner.execute(/* skip_tenant_check */
                        """
                        UPDATE cms_member m
                        SET status='ACTIVE', _modified=NOW()
                        FROM cms_account_register r
                        WHERE r.member_id = m.id
                          AND r.spjangcd   = :spjangcd
                          AND r.member_no  = :memberNo
                          AND TO_CHAR(r.eb13_sent_at, 'YYYYMMDD') = :targetDate
                          AND r.apply_type = '3'
                          AND COALESCE(r.change_flag,'N') <> 'Y'
                          AND m.status = 'PENDING_CANCEL'
                        """, p);
                log.info("[CmsEb14] 해지불능 → cms_member ACTIVE 원복 memberNo={} code={}", memberNo, failCode);
            }
        }

        // ── 정상(파일에 불능으로 안 나온 건) 처리 ────────────────────────────
        // ★ 제외 기준도 (납부자번호, 신청구분) 쌍이어야 한다.
        //   member_no 만으로 제외하면, 해지만 불능인 계좌변경 건에서 승인된 신규행이
        //   영원히 PENDING 으로 남는 유령 행이 된다.
        List<String> failNewNos    = new ArrayList<>();   // 신규('1') 불능 납부자번호
        List<String> failCancelNos = new ArrayList<>();   // 해지('3') 불능 납부자번호
        for (String k : failCodeMap.keySet()) {
            String[] parts = k.split("\\|", 2);
            if ("3".equals(parts[1])) failCancelNos.add(parts[0]);
            else                      failNewNos.add(parts[0]);
        }

        var base = new MapSqlParameterSource();
        base.addValue("spjangcd",   spjangcd);
        base.addValue("targetDate", targetDate);

        // 신규(apply_type='1') 정상 → APPROVED
        var pNew = new MapSqlParameterSource(base.getValues());
        String notInNew = "";
        if (!failNewNos.isEmpty()) {
            notInNew = "AND member_no NOT IN (:failNewNos) ";
            pNew.addValue("failNewNos", failNewNos);
        }
        sqlRunner.execute(/* skip_tenant_check */
                "UPDATE cms_account_register " +
                        "SET eb14_received_at=NOW(), eb14_result='Y', status='APPROVED', _modified=NOW() " +
                        "WHERE spjangcd=:spjangcd AND " + SENT_DATE_EXPR + " = :targetDate " +
                        "  AND COALESCE(apply_type,'1') = '1' " +
                        notInNew +
                        "  AND eb13_status='SENT' AND status='PENDING'",
                pNew);

        // 해지(apply_type='3') 정상 → CANCELLED
        var pCancel = new MapSqlParameterSource(base.getValues());
        String notInCancel = "";
        if (!failCancelNos.isEmpty()) {
            notInCancel = "AND member_no NOT IN (:failCancelNos) ";
            pCancel.addValue("failCancelNos", failCancelNos);
        }
        sqlRunner.execute(/* skip_tenant_check */
                "UPDATE cms_account_register " +
                        "SET eb14_received_at=NOW(), eb14_result='Y', status='CANCELLED', _modified=NOW() " +
                        "WHERE spjangcd=:spjangcd AND " + SENT_DATE_EXPR + " = :targetDate " +
                        "  AND apply_type='3' " +
                        notInCancel +
                        "  AND eb13_status='SENT' AND status='PENDING'",
                pCancel);

        // ── cms_member 상태 반영 ─────────────────────────────────────────────
        // 신규 승인 → agree_yn='Y'
        sqlRunner.execute(/* skip_tenant_check */
                """
                UPDATE cms_member m
                SET agree_yn='Y', agree_date=NOW(), _modified=NOW()
                FROM cms_account_register r
                WHERE r.member_id   = m.id
                  AND r.spjangcd    = :spjangcd
                  AND TO_CHAR(r.eb13_sent_at, 'YYYYMMDD') = :targetDate
                  AND r.status      = 'APPROVED'
                """, base);

        // 해지 완료 → INACTIVE 확정 (단, 계좌변경 해지건은 제외 — 회원은 살아있어야 함)
        sqlRunner.execute(/* skip_tenant_check */
                """
                UPDATE cms_member m
                SET status='INACTIVE', _modified=NOW()
                FROM cms_account_register r
                WHERE r.member_id   = m.id
                  AND r.spjangcd    = :spjangcd
                  AND TO_CHAR(r.eb13_sent_at, 'YYYYMMDD') = :targetDate
                  AND r.status      = 'CANCELLED'
                  AND COALESCE(r.change_flag,'N') <> 'Y'
                """, base);

        // ── 계좌변경 신규건 승인 → cms_member 계좌를 새 값으로 갱신(활성 유지) ──
        sqlRunner.execute(/* skip_tenant_check */
                """
                UPDATE cms_member m
                SET bank_code      = r.bank_code,
                    bank_account   = r.bank_account,
                    account_holder = COALESCE(r.account_holder, m.account_holder),
                    status         = 'ACTIVE',
                    agree_yn       = 'Y',
                    agree_date     = NOW(),
                    _modified      = NOW()
                FROM cms_account_register r
                WHERE r.member_id   = m.id
                  AND r.spjangcd    = :spjangcd
                  AND TO_CHAR(r.eb13_sent_at, 'YYYYMMDD') = :targetDate
                  AND r.status      = 'APPROVED'
                  AND r.apply_type  = '1'
                  AND r.change_flag = 'Y'
                """, base);

        // ── cms_file_register 연결 ──────────────────────────────────────────
        // 재수신 시 중복 INSERT 되지 않도록 이미 연결된 건은 제외한다.
        List<Map<String, Object>> registers = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT r.id
                FROM cms_account_register r
                WHERE r.spjangcd = :spjangcd
                  AND TO_CHAR(r.eb13_sent_at, 'YYYYMMDD') = :targetDate
                  AND r.eb13_status = 'SENT'
                  AND NOT EXISTS (
                        SELECT 1 FROM cms_file_register fr
                        WHERE fr.file_id = :fileId AND fr.register_id = r.id
                  )
                ORDER BY r.id
                """,
                new MapSqlParameterSource("spjangcd", spjangcd)
                        .addValue("targetDate", targetDate)
                        .addValue("fileId", fileId));

        int seq = 1;
        for (Map<String, Object> r : registers) {
            long registerId = ((Number) r.get("id")).longValue();
            sqlRunner.execute(/* skip_tenant_check */
                    """
                    INSERT INTO cms_file_register (file_id, register_id, line_seq)
                    VALUES (:fileId, :registerId, :seq)
                    """,
                    new MapSqlParameterSource("fileId", fileId)
                            .addValue("registerId", registerId)
                            .addValue("seq", seq++));
        }

        log.info("[CmsEb14] 처리완료 spjangcd={} date={} 불능={}건(신규 {} / 해지 {})",
                spjangcd, targetDate, failCodeMap.size(), failNewNos.size(), failCancelNos.size());
    }

    private byte[] sftpDownload(String fileName, String user, String password) {
        JSch jsch = new JSch();
        try {
            Session session = jsch.getSession(user, sftpHost, sftpPort);
            session.setPassword(password);
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect(15000);
            ChannelSftp channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect(10000);

            try (var baos = new ByteArrayOutputStream()) {
                try {
                    channel.get(fileName, baos);
                } catch (SftpException e) {
                    String msg = e.getMessage() != null ? e.getMessage() : "";
                    if (msg.contains("inputstream is closed") || msg.contains("End of IO")) {
                        log.warn("[CmsEb14] SFTP 서버 강제종료(정상) - 수신 데이터: {}bytes", baos.size());
                        if (baos.size() > 0) return baos.toByteArray();
                    } else if (msg.contains("No such file")) {
                        return null;
                    }
                    throw e;
                }
                return baos.toByteArray();
            } finally {
                try { channel.disconnect(); } catch (Exception ignored) {}
                try { session.disconnect(); } catch (Exception ignored) {}
            }
        } catch (Exception e) {
            log.error("[CmsEb14] SFTP 다운로드 실패: {}", e.getMessage());
            return null;
        }
    }
}