package mes.app.Scheduler.SchedulerService;

import com.jcraft.jsch.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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

    private static final String FEATURE_CODE = "EB_FILE";
    private static final Charset EUC_KR = Charset.forName("EUC-KR");

    private final SqlRunner sqlRunner;
    private final NcpObjectStorageService storageService;
    private final CmsTokenService cmsTokenService;

    @Value("${cms.sftp-host:tsftp.cmsedi.or.kr}")
    private String sftpHost;

    @Value("${cms.sftp-port:11133}")
    private int sftpPort;

    public void run() {
        LocalDate yesterday = LocalDate.now().minusDays(1);
        String targetDate = yesterday.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String mmdd = targetDate.substring(4, 8);
        String yyyy = targetDate.substring(0, 4);
        String ec22FileName = "EC22" + mmdd + "_" + yyyy;

        log.info("[CmsEc22Receive] 시작 - 출금대상일: {}, 파일: {}", targetDate, ec22FileName);

        List<Map<String, Object>> spjangs = sqlRunner.getRows(/* skip_tenant_check */
                "SELECT DISTINCT spjangcd FROM cms_billing WHERE deduct_date=:td AND status='REQUESTED' AND deduct_type='EC'",
                new MapSqlParameterSource("td", targetDate));

        if (spjangs.isEmpty()) {
            log.info("[CmsEc22Receive] REQUESTED 청구 없음 - 종료");
            return;
        }

        for (Map<String, Object> row : spjangs) {
            String spjangcd = str(row.get("spjangcd"));
            try {
                processSpjang(spjangcd, targetDate, ec22FileName);
            } catch (Exception e) {
                log.error("[CmsEc22Receive] 실패 spjangcd={}: {}", spjangcd, e.getMessage(), e);
            }
        }
    }

    private void processSpjang(String spjangcd, String targetDate, String ec22FileName) throws Exception {
        byte[] fileBytes;
        try {
            fileBytes = sftpDownloadWithApiCredential(ec22FileName, targetDate, spjangcd);
        } catch (Exception e) {
            log.warn("[CmsEc22Receive] EC22 파일 없음 또는 수신 실패 spjangcd={}: {}", spjangcd, e.getMessage());
            markAllSuccess(targetDate, spjangcd);
            return;
        }

        try (ByteArrayInputStream bis = new ByteArrayInputStream(fileBytes)) {
            String objectKey = "cms/result/" + spjangcd + "/" + ec22FileName;
            storageService.upload(objectKey, bis, fileBytes.length, "text/plain");
            sqlRunner.execute(/* skip_tenant_check */
                    """
                    INSERT INTO cms_file (
                        spjangcd, file_name, file_type, file_path,
                        target_date, billing_count, billing_amount,
                        send_status, _creater_id, _created, _modifier_id, _modified
                    ) VALUES (
                        :spjangcd, :fileName, 'EC_RESULT', :filePath,
                        CAST(:targetDate AS DATE), 0, 0,
                        'RECEIVED', 'SYSTEM', NOW(), 'SYSTEM', NOW()
                    )
                    """,
                    new MapSqlParameterSource()
                            .addValue("spjangcd",   spjangcd)
                            .addValue("fileName",   ec22FileName)
                            .addValue("filePath",   objectKey)
                            .addValue("targetDate", targetDate));
        } catch (Exception e) {
            log.warn("[CmsEc22Receive] NCP 업로드 실패 (처리는 계속): {}", e.getMessage());
        }

        Map<String, String> failMap = parseEc22(fileBytes);
        log.info("[CmsEc22Receive] 불능 건수 spjangcd={}: {}", spjangcd, failMap.size());

        List<Map<String, Object>> requestedBillings = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT b.id, m.member_no
                FROM cms_billing b
                LEFT JOIN cms_member m ON m.id = b.member_id
                WHERE b.deduct_date = :targetDate
                  AND b.status      = 'REQUESTED'
                  AND b.spjangcd    = :spjangcd
                """,
                new MapSqlParameterSource("targetDate", targetDate).addValue("spjangcd", spjangcd));

        int successCount = 0, failCount = 0;
        for (Map<String, Object> b : requestedBillings) {
            long   billingId = ((Number) b.get("id")).longValue();
            String memberNo  = str(b.get("member_no"));
            var    p         = new MapSqlParameterSource("billingId", billingId);
            p.addValue("resultDate", targetDate);

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
                failCount++;
            } else {
                sqlRunner.execute(/* skip_tenant_check */
                        """
                        UPDATE cms_billing SET status='SUCCESS', result_code='0000',
                            result_msg='출금성공', result_date=:resultDate, _modified=NOW()
                        WHERE id=:billingId AND status='REQUESTED'
                        """, p);
                successCount++;
            }
        }
        log.info("[CmsEc22Receive] 완료 spjangcd={} 성공={}건 실패={}건", spjangcd, successCount, failCount);
    }

    /** EC22 파일이 없을 때 — 전체 REQUESTED → SUCCESS */
    private void markAllSuccess(String targetDate, String spjangcd) {
        String today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        var p = new MapSqlParameterSource();
        p.addValue("targetDate", targetDate);
        p.addValue("resultDate", today);
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
        String[] lines = new String(fileBytes, EUC_KR).split("\n");
        for (String line : lines) {
            byte[] lineBytes = line.getBytes(EUC_KR);
            if (lineBytes.length < 111) continue;
            char recType = (char) (lineBytes[0] & 0xFF);
            if (recType != 'R') continue;

            // 출금결과 (pos 69-73, 0-based: 68~72) — 출금여부(1) + 불능코드(4)
            String resultField = new String(Arrays.copyOfRange(lineBytes, 68, 73), EUC_KR).trim();
            String resultCode  = resultField.length() >= 4 ? resultField.substring(1) : resultField;

            // 납부자번호 (pos 92-111, 0-based: 91~110)
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
        channel.connect(10000);
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            channel.get(fileName, baos);
            return baos.toByteArray();
        } finally {
            try { channel.disconnect(); } catch (Exception ignored) {}
            try { session.disconnect(); } catch (Exception ignored) {}
        }
    }

    private String resolveFailMsg(String code) {
        if (code == null) return "출금실패";
        return switch (code) {
            case "0021" -> "기준금액 미만 출금불능";
            case "0031" -> "잔액부족";
            case "0051" -> "계좌해지";
            case "0052" -> "계좌정지";
            case "0061" -> "예금주 상이";
            case "0071" -> "계좌번호 오류";
            default     -> "출금실패(" + code + ")";
        };
    }

    private String str(Object v) { return v != null ? v.toString() : ""; }
}
