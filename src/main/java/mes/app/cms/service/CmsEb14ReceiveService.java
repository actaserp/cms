package mes.app.cms.service;

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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * EB14 — 출금이체 신청결과 수신
 * 불능분만 전송됨 (정상 건은 파일에 없음)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CmsEb14ReceiveService {

    private static final String FEATURE_CODE = "EB14";
    private static final Charset EUC_KR = Charset.forName("EUC-KR");

    private final SqlRunner sqlRunner;
    private final CmsTokenService cmsTokenService;
    private final NcpObjectStorageService storageService;

    @Value("${cms.sftp-host}")
    private String sftpHost;

    @Value("${cms.sftp-port}")
    private int sftpPort;

    public void run() {
        // EB13 송신한 모든 사업장 대상
        List<Map<String, Object>> spjangs = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT DISTINCT spjangcd FROM cms_account_register
                WHERE eb13_status = 'SENT'
                  AND eb14_received_at IS NULL
                  AND eb13_sent_at >= NOW() - INTERVAL '3 days'
                """,
                new MapSqlParameterSource());

        for (Map<String, Object> row : spjangs) {
            String spjangcd = (String) row.get("spjangcd");
            try {
                receive(spjangcd);
            } catch (Exception e) {
                log.error("[CmsEb14] 수신 실패 spjangcd={}: {}", spjangcd, e.getMessage(), e);
            }
        }
    }

    public void receive(String spjangcd) throws Exception {
        String targetDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String mmdd = targetDate.substring(4, 8);
        String fileName = "EB14" + mmdd + "_" + targetDate.substring(0, 4);

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

        // cms_file INSERT
        var fp = new MapSqlParameterSource();
        fp.addValue("spjangcd",  spjangcd);
        fp.addValue("fileName",  fileName);
        fp.addValue("filePath",  objectKey);
        fp.addValue("targetDate", targetDate);
        Map<String, Object> fileRow = sqlRunner.getRow(/* skip_tenant_check */
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
        long fileId = ((Number) fileRow.get("id")).longValue();

        parseAndUpdate(spjangcd, fileBytes, targetDate, fileId);
    }

    private void parseAndUpdate(String spjangcd, byte[] fileBytes, String targetDate, long fileId) {
        // 120 bytes 단위로 파싱
        int recordSize = 120;
        int totalRecords = fileBytes.length / recordSize;

        List<String> failMemberNos = new java.util.ArrayList<>();
        List<String> failCodes     = new java.util.ArrayList<>();

        for (int i = 0; i < totalRecords; i++) {
            byte[] record = Arrays.copyOfRange(fileBytes, i * recordSize, (i + 1) * recordSize);
            String line = new String(record, EUC_KR);

            String recordType = line.substring(0, 1);
            if (!"R".equals(recordType)) continue;

            // Data Record 파싱
            // 신청구분(6번, pos=25, len=1) 납부자번호(6번, pos=26, len=20)
            // 처리결과코드(12번 결과코드, pos=89, len=1) 불능코드(pos=90, len=4)
            String memberNo  = line.substring(26, 46).trim();
            String resultCd  = line.substring(91, 92).trim();  // 89 → 91
            String failCode  = line.substring(92, 96).trim();  // 90 → 92

            if ("N".equals(resultCd)) {
                failMemberNos.add(memberNo);
                failCodes.add(failCode);
                log.info("[CmsEb14] 불능 memberNo={} code={}", memberNo, failCode);
            }
        }

        // 불능 건 업데이트
        for (int i = 0; i < failMemberNos.size(); i++) {
            String memberNo = failMemberNos.get(i);
            String failCode = failCodes.get(i);
            var param = new MapSqlParameterSource();
            param.addValue("spjangcd", spjangcd);
            param.addValue("memberNo", memberNo);
            param.addValue("failCode", failCode);
            param.addValue("targetDate", targetDate);
            sqlRunner.execute(/* skip_tenant_check */
                    """
                    UPDATE cms_account_register
                    SET eb14_result='N', eb14_fail_code=:failCode,
                        eb14_received_at=NOW(), status='REJECTED', _modified=NOW()
                    WHERE spjangcd=:spjangcd AND member_no=:memberNo
                      AND apply_date=:targetDate
                    """, param);
        }

        // 불능 아닌 건 → APPROVED + agree_yn = 'Y'
        var param = new MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);
        param.addValue("targetDate", targetDate);
        if (!failMemberNos.isEmpty()) {
            param.addValue("failNos", failMemberNos);
            sqlRunner.execute(/* skip_tenant_check */
                    """
                    UPDATE cms_account_register
                    SET eb14_received_at=NOW(), eb14_result='Y', status='APPROVED', _modified=NOW()
                    WHERE spjangcd=:spjangcd AND apply_date=:targetDate
                      AND member_no NOT IN (:failNos)
                      AND eb13_status='SENT' AND status='PENDING'
                    """, param);
        } else {
            sqlRunner.execute(/* skip_tenant_check */
                    """
                    UPDATE cms_account_register
                    SET eb14_received_at=NOW(), eb14_result='Y', status='APPROVED', _modified=NOW()
                    WHERE spjangcd=:spjangcd AND apply_date=:targetDate
                      AND eb13_status='SENT' AND status='PENDING'
                    """, param);
        }

        // cms_member.agree_yn = 'Y' 업데이트
        sqlRunner.execute(/* skip_tenant_check */
                """
                UPDATE cms_member m
                SET agree_yn='Y', agree_date=NOW(), _modified=NOW()
                FROM cms_account_register r
                WHERE r.member_id = m.id
                  AND r.spjangcd = :spjangcd
                  AND r.apply_date = :targetDate
                  AND r.status = 'APPROVED'
                """, param);

        List<Map<String, Object>> registers = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT id FROM cms_account_register
                WHERE spjangcd=:spjangcd AND apply_date=:targetDate
                  AND eb13_status='SENT'
                """,
                new MapSqlParameterSource("spjangcd", spjangcd)
                        .addValue("targetDate", targetDate));

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

        log.info("[CmsEb14] 처리완료 spjangcd={} 불능={}건", spjangcd, failMemberNos.size());
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