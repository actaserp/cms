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
 * EB13 — 출금이체 신청내역 송신 (기관접수)
 * 파일포맷: 120 Bytes
 * EI13 송신 후 24시간 이내 송신해야 함
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CmsEb13SendService {

    private static final String FEATURE_CODE = "EB_FILE";
    private static final Charset EUC_KR = Charset.forName("EUC-KR");
    private String padLeft(String s, int n) { return padLeft(s, n, ' '); }

    private final SqlRunner sqlRunner;
    private final NcpObjectStorageService storageService;
    private final CmsTokenService cmsTokenService;

    @Value("${cms.sftp-host}")
    private String sftpHost;

    @Value("${cms.sftp-port}")
    private int sftpPort;

    public Map<String, Object> send(List<Long> registerIds) {
        String spjangcd = TenantContext.get();

        Map<String, Object> xa012 = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT cms_code FROM tb_xa012_cms WHERE spjangcd=:s",
                new MapSqlParameterSource("s", spjangcd));
        String institutionCode = xa012 != null ? str(xa012.get("cms_code")) : "";
        if (!StringUtils.hasText(institutionCode)) {
            log.error("[CmsEb13] cms_code 없음 spjangcd={}", spjangcd);
            return Map.of("sent", 0, "failed", 0, "message", "cms_code 미설정");
        }

        List<Map<String, Object>> targets = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT r.id, r.member_no, r.bank_code, r.bank_account,
                       r.id_number, r.member_type, r.apply_date, r.apply_type,
                       r.ei13_status
                FROM cms_account_register r
                WHERE r.id IN (:ids)
                  AND r.spjangcd = :spjangcd
                  AND r.ei13_status = 'SENT'
                  AND r.eb13_status IN ('PENDING', 'FAILED')
                """,
                new MapSqlParameterSource("ids", registerIds).addValue("spjangcd", spjangcd));

        if (targets.isEmpty()) {
            return Map.of("sent", 0, "failed", 0, "message", "전송 대상 없음 (EI13 미송신 건 제외)");
        }

        String applyDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String mmdd = applyDate.substring(4, 8);
        String fileName = "EB13" + mmdd;

        int sent = 0, failed = 0;
        try {
            byte[] fileBytes = buildEb13File(targets, applyDate, institutionCode);

            // NCP 업로드
            String objectKey = storageService.buildObjectKey(spjangcd, FEATURE_CODE, fileName);
            try (var bis = new ByteArrayInputStream(fileBytes)) {
                storageService.upload(objectKey, bis, fileBytes.length, "text/plain");
            }

            // SFTP 송신
            String[] cred = cmsTokenService.getSftpSendCredential(spjangcd, "EB13", applyDate);
            sftpUpload(fileBytes, fileName, cred[0], cred[1]);

            // 상태 업데이트
            var param = new MapSqlParameterSource();
            param.addValue("ids", registerIds);
            param.addValue("filePath", objectKey);
            sqlRunner.execute(/* skip_tenant_check */
                    """
                    UPDATE cms_account_register
                    SET eb13_status='SENT', eb13_sent_at=NOW(),
                        eb13_file_path=:filePath, _modified=NOW()
                    WHERE id IN (:ids)
                    """, param);

            sent = targets.size();
            log.info("[CmsEb13] 송신 완료 spjangcd={} {}건", spjangcd, sent);

        } catch (Exception e) {
            failed = targets.size();
            log.error("[CmsEi13] 송신 실패 spjangcd={}: {}", spjangcd, e.getMessage(), e);

            var param = new MapSqlParameterSource();
            param.addValue("ids", registerIds);
            param.addValue("errMsg", e.getMessage());
            param.addValue("applyDate", applyDate); // 추가
            sqlRunner.execute(/* skip_tenant_check */
                    """
                    UPDATE cms_account_register
                    SET eb13_status='FAILED', memo=:errMsg,
                        status='FAILED',
                        _modified=NOW()
                    WHERE id IN (:ids)
                    """, param);
        }

        return Map.of("sent", sent, "failed", failed);
    }

    private byte[] buildEb13File(List<Map<String, Object>> targets, String applyDate, String institutionCode) throws IOException {
        String orgCode = padRight(institutionCode, 10);
        String mmdd = applyDate.substring(4, 8);
        String yymmdd = applyDate.substring(2, 8);
        String fileName = "EB13" + mmdd;

        int newCount = 0;
        long totalCount = targets.size();
        var baos = new ByteArrayOutputStream();

        // ── Header (120 bytes) ───────────────────────────────────────────
        // Record구분(1)+일련번호(8)+기관코드(10)+파일명(8)+신청일자(6)+FILLER(87)
        StringBuilder h = new StringBuilder();
        h.append("H");
        h.append("00000000");
        h.append(orgCode);
        h.append(padRight(fileName, 8));
        h.append(yymmdd);
        baos.write(toFixed(h.toString(), 120));

        // ── Data Records (120 bytes each) ────────────────────────────────
        // Record구분(1)+일련번호(8)+기관코드(10)+신청일자(6)+신청구분(1)+납부자번호(20)
        // +은행점코드(7)+계좌번호(16)+생년월일or사업자번호(16)+취급점코드(4)
        // +자금종류(2)+처리결과코드(1)+불능코드(4)+FILLER(1)+기관사용영역(12)+접수채널(1)+FILLER(10)
        int seq = 1;
        for (Map<String, Object> t : targets) {
            String applyType = str(t.get("apply_type"));
            if (!StringUtils.hasText(applyType)) applyType = "1";
            if ("1".equals(applyType)) newCount++;

            String bankCode  = padLeft(str(t.get("bank_code")), 3, '0');
            String idNum     = padRight(str(t.get("id_number")), 16);

            StringBuilder r = new StringBuilder();
            r.append("R");
            r.append(padLeft(String.valueOf(seq++), 8, '0'));
            r.append(orgCode);
            r.append(yymmdd);                              // 신청일자 (6)
            r.append(applyType);                           // 신청구분 (1)
            r.append(padRight(str(t.get("member_no")), 20)); // 납부자번호 (20)
            r.append(bankCode).append("0000");             // 은행점코드 (7)
            r.append(padLeft(str(t.get("bank_account")).replaceAll("-", ""), 16)); // 계좌번호 (16)
            r.append(idNum);                               // 생년월일or사업자번호 (16)
            r.append(spaces(4));                           // 취급점코드 (4) Space
            r.append(spaces(2));                           // 자금종류 (2)
            r.append(" ");                                 // 처리결과코드 (1) Space
            r.append(spaces(4));                           // 불능코드 (4) Space
            r.append(" ");                                 // FILLER (1)
            r.append(spaces(12));                          // 기관사용영역 (12)
            r.append(" ");                                 // 접수채널 (1) Space
            r.append(spaces(10));                          // FILLER (10)
            baos.write(toFixed(r.toString(), 120));
        }

        // ── Trailer (120 bytes) ──────────────────────────────────────────
        // Record구분(1)+일련번호(8)+기관코드(10)+파일명(8)+총Data수(8)
        // +신규건수(8)+변경건수(8)+해지건수(8)+임의해지건수(8)+FILLER(43)+MAC(10)
        StringBuilder tr = new StringBuilder();
        tr.append("T");
        tr.append("99999999");
        tr.append(orgCode);
        tr.append(padRight(fileName, 8));
        tr.append(padLeft(String.valueOf(totalCount), 8, '0')); // 총 Data 수
        tr.append(padLeft(String.valueOf(newCount), 8, '0'));    // 신규
        tr.append("00000000");                                   // 변경
        tr.append("00000000");                                   // 해지
        tr.append("00000000");                                   // 임의해지
        tr.append(spaces(43));                                   // FILLER
        tr.append(spaces(10));                                   // MAC (미사용 시 Space)
        baos.write(toFixed(tr.toString(), 120));

        return baos.toByteArray();
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
            log.info("[CmsEb13] SFTP 업로드 완료: {}", fileName);
        } catch (SftpException e) {
            if (e.getMessage() != null && e.getMessage().contains("End of IO")) {
                log.warn("[CmsEb13] SFTP 서버 강제종료(정상): {}", e.getMessage());
            } else {
                throw e;
            }
        } finally {
            try { channel.disconnect(); } catch (Exception ignored) {}
            try { session.disconnect(); } catch (Exception ignored) {}
        }
    }

    private byte[] toFixed(String s, int len) {
        byte[] b = s.getBytes(EUC_KR);
        byte[] result = new byte[len];
        Arrays.fill(result, (byte) ' ');
        System.arraycopy(b, 0, result, 0, Math.min(b.length, len));
        return result;
    }

    private String str(Object v)              { return v != null ? v.toString() : ""; }
    private String spaces(int n)              { return " ".repeat(n); }
    private String padRight(String s, int n)  { return String.format("%-" + n + "s", s != null ? s : "").substring(0, n); }
    private String padLeft(String s, int n, char c) {
        String v = s != null ? s : "";
        if (v.length() >= n) return v.substring(v.length() - n);
        return String.valueOf(c).repeat(n - v.length()) + v;
    }
}