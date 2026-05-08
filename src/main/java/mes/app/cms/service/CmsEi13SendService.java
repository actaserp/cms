package mes.app.cms.service;

import com.jcraft.jsch.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mes.app.Scheduler.SchedulerService.CmsEb21SendService;
import mes.app.common.TenantContext;
import mes.app.files.NcpObjectStorageService;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * EI13 — 자동납부 동의자료 송신
 * 파일포맷: 1024 Bytes 단위 블록
 * Header(1024) + Data Record(n*1024) + Trailer(1024)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CmsEi13SendService {

    private static final String FEATURE_CODE = "EB_FILE";
    private static final Charset EUC_KR = Charset.forName("EUC-KR");

    private final SqlRunner sqlRunner;
    private final NcpObjectStorageService storageService;
    private final CmsTokenService cmsTokenService;

    @Value("${cms.sftp-host}")
    private String sftpHost;

    @Value("${cms.sftp-port}")
    private int sftpPort;

    /**
     * 특정 account_register 건들 EI13 송신
     */
    public Map<String, Object> send(List<Long> registerIds) {
        String spjangcd = TenantContext.get();

        Map<String, Object> xa012 = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT cms_org_code FROM tb_xa012 WHERE spjangcd=:s",
                new MapSqlParameterSource("s", spjangcd));
        String institutionCode = xa012 != null ? str(xa012.get("cms_org_code")) : "";
        if (!StringUtils.hasText(institutionCode)) {
            log.error("[CmsEi13] cms_org_code 없음 spjangcd={}", spjangcd);
            return Map.of("sent", 0, "failed", registerIds.size(), "message", "cms_org_code 미설정");
        }

        List<Map<String, Object>> targets = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT r.id, r.member_id, r.member_no, r.bank_code, r.bank_account,
                       r.apply_date, r.agree_type, r.agree_ext, r.agree_file_path,
                       r.id_number, r.member_type
                FROM cms_account_register r
                WHERE r.id IN (:ids)
                  AND r.spjangcd = :spjangcd
                  AND r.ei13_status IN ('PENDING', 'FAILED')
                """,
                new MapSqlParameterSource("ids", registerIds).addValue("spjangcd", spjangcd));

        if (targets.isEmpty()) {
            return Map.of("sent", 0, "failed", 0, "message", "전송 대상 없음");
        }

        String applyDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String mmdd = applyDate.substring(4, 8);
        String fileName = "EI13" + mmdd;

        int sent = 0, failed = 0;
        try {
            byte[] fileBytes = buildEi13File(spjangcd, targets, applyDate, institutionCode);

            // NCP 업로드
            String objectKey = storageService.buildObjectKey(spjangcd, FEATURE_CODE, fileName);
            try (var bis = new ByteArrayInputStream(fileBytes)) {
                storageService.upload(objectKey, bis, fileBytes.length, "application/octet-stream");
            }

            // SFTP 송신
            String[] cred = cmsTokenService.getSftpSendCredential(spjangcd, "EI13", applyDate);
            sftpUpload(fileBytes, fileName, cred[0], cred[1]);

            // 상태 업데이트
            var param = new MapSqlParameterSource();
            param.addValue("ids", registerIds);
            param.addValue("filePath", objectKey);
            param.addValue("applyDate", applyDate);
            sqlRunner.execute(/* skip_tenant_check */
                    """
                    UPDATE cms_account_register
                    SET ei13_status='SENT', ei13_sent_at=NOW(),
                        ei13_file_path=:filePath, apply_date=:applyDate, _modified=NOW()
                    WHERE id IN (:ids)
                    """, param);

            sent = targets.size();
            log.info("[CmsEi13] 송신 완료 spjangcd={} {}건", spjangcd, sent);

        } catch (Exception e) {
            failed = targets.size();
            log.error("[CmsEi13] 송신 실패 spjangcd={}: {}", spjangcd, e.getMessage(), e);

            var param = new MapSqlParameterSource();
            param.addValue("ids", registerIds);
            param.addValue("errMsg", e.getMessage());
            param.addValue("applyDate", applyDate);
            sqlRunner.execute(/* skip_tenant_check */
                    """
                    UPDATE cms_account_register
                    SET ei13_status='FAILED', memo=:errMsg,
                        ei13_sent_at=NOW(),
                        apply_date=:applyDate,
                        status='FAILED',
                        _modified=NOW()
                    WHERE id IN (:ids)
                    """, param);
        }

        return Map.of("sent", sent, "failed", failed);
    }

    private byte[] buildEi13File(String spjangcd, List<Map<String, Object>> targets, String applyDate, String institutionCode) throws Exception {
        String orgCode = padRight(institutionCode, 10);
        String orgCode20 = padRight(institutionCode, 20);
        int totalCount = targets.size();

        var baos = new ByteArrayOutputStream();

        // ── Header (1024 bytes) ──────────────────────────────────────────
        // 업무구분코드(6) + Record구분(2) + 일련번호(7) + 신청일(8) + 기관코드(20) + 총동의자료수(7) + FILLER(974)
        StringBuilder h = new StringBuilder();
        h.append("AE1112");
        h.append("11");
        h.append("0000000");
        h.append(applyDate);
        h.append(orgCode20);
        h.append(padLeft(String.valueOf(totalCount), 7, '0'));
        baos.write(toPaddedBlock(h.toString(), 1024));

        // ── Data Records ────────────────────────────────────────────────
        int seq = 1;
        int totalBlocks = 0;

        for (Map<String, Object> target : targets) {
            String memberNo   = padRight(str(target.get("member_no")), 30);
            String bankCode   = padLeft(str(target.get("bank_code")), 3, '0');
            String bankAcct   = padRight(str(target.get("bank_account")).replaceAll("-", ""), 20);
            String agreeType  = str(target.get("agree_type"));
            if (!StringUtils.hasText(agreeType)) agreeType = "1";
            String agreeExt   = padRight(str(target.get("agree_ext")), 5);

            // 동의자료 파일 다운로드 (NCP)
            byte[] agreeFileBytes = new byte[0];
            String filePath = str(target.get("agree_file_path"));
            if (StringUtils.hasText(filePath)) {
                try (var stream = storageService.download(filePath)) {
                    agreeFileBytes = stream.readAllBytes();
                }
            }
            int agreeLen = agreeFileBytes.length;

            // 식별정보 119 bytes
            // 업무구분(6)+Record구분(2)+일련번호(7)+FILLER(10)+기관코드(20)+납부자번호(30)
            // +은행코드(3)+계좌번호(20)+신청일(8)+동의자료구분(1)+동의자료확장자(5)+동의자료길이(7)
            StringBuilder id = new StringBuilder();
            id.append("AE1112");
            id.append("22");
            id.append(padLeft(String.valueOf(seq++), 7, '0'));
            id.append(spaces(10));
            id.append(orgCode20);
            id.append(memberNo);
            id.append(bankCode);
            id.append(bankAcct);
            id.append(applyDate);
            id.append(agreeType);
            id.append(agreeExt);
            id.append(padLeft(String.valueOf(agreeLen), 7, '0'));
            // 식별정보 = 119 bytes

            byte[] idBytes = toEucKrBytes(id.toString(), 119);

            // 식별정보 + 동의자료 합쳐서 1024 블록으로 분할
            byte[] combined = new byte[idBytes.length + agreeFileBytes.length];
            System.arraycopy(idBytes, 0, combined, 0, idBytes.length);
            System.arraycopy(agreeFileBytes, 0, combined, idBytes.length, agreeFileBytes.length);

            // FILLER 계산: (1024 - (119 + agreeLen) % 1024) % 1024
            int fillerLen = (1024 - (119 + agreeLen) % 1024) % 1024;
            byte[] filler = new byte[fillerLen];
            Arrays.fill(filler, (byte) ' ');

            baos.write(combined);
            baos.write(filler);

            int blocks = (combined.length + fillerLen) / 1024;
            totalBlocks += blocks;
        }

        // ── Trailer (1024 bytes) ─────────────────────────────────────────
        // 업무구분(6)+Record구분(2)+일련번호(7)+기관코드(20)+총Data수(7)+총Block수(10)+FILLER(972)
        StringBuilder t = new StringBuilder();
        t.append("AE1112");
        t.append("33");
        t.append("9999999");
        t.append(orgCode20);
        t.append(padLeft(String.valueOf(totalCount), 7, '0'));
        t.append(padLeft(String.valueOf(totalBlocks), 10, '0'));
        baos.write(toPaddedBlock(t.toString(), 1024));

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
            log.info("[CmsEi13] SFTP 업로드 완료: {}", fileName);
        } catch (SftpException e) {
            if (e.getMessage() != null && e.getMessage().contains("End of IO")) {
                log.warn("[CmsEi13] SFTP 서버 강제종료(정상): {}", e.getMessage());
            } else {
                throw e;
            }
        } finally {
            try { channel.disconnect(); } catch (Exception ignored) {}
            try { session.disconnect(); } catch (Exception ignored) {}
        }
    }

    // ── 헬퍼 ────────────────────────────────────────────────────────────

    private byte[] toPaddedBlock(String s, int blockSize) {
        byte[] b = s.getBytes(EUC_KR);
        byte[] result = new byte[blockSize];
        Arrays.fill(result, (byte) ' ');
        System.arraycopy(b, 0, result, 0, Math.min(b.length, blockSize));
        return result;
    }

    private byte[] toEucKrBytes(String s, int len) {
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