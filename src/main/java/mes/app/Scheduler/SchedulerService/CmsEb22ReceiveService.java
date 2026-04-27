package mes.app.Scheduler.SchedulerService;

import com.jcraft.jsch.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mes.app.files.NcpObjectStorageService;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

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
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CmsEb22ReceiveService {

    private static final String FEATURE_CODE = "EB_FILE";
    private static final Charset EUC_KR = Charset.forName("EUC-KR");

    private final SqlRunner sqlRunner;
    private final NcpObjectStorageService storageService;
    private final CmsEbFileGenerateService cmsEbFileGenerateService;

    @Value("${cms.sftp-host:tsftp.cmsedi.or.kr}")
    private String sftpHost;

    @Value("${cms.sftp-port:11133}")
    private int sftpPort;

    public void run() {
        // D-1 출금일 = 어제 (오늘 새벽에 어제 출금 결과 수신)
        LocalDate yesterday = LocalDate.now().minusDays(1);
        String targetDate = yesterday.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String mmdd = targetDate.substring(4, 8);
        String yyyy = targetDate.substring(0, 4);
        // 파일명: EB22{MMDD}_{YYYY} (금결원 규격)
        String eb22FileName = "EB22" + mmdd + "_" + yyyy;

        log.info("[CmsEb22Receive] 시작 - 출금대상일: {}, 파일: {}", targetDate, eb22FileName);

        // 1. SFTP에서 EB22 파일 수신 (1회용 계정 획득 후)
        byte[] fileBytes;
        try {
            fileBytes = sftpDownloadWithApiCredential(eb22FileName, targetDate);
        } catch (Exception e) {
            log.warn("[CmsEb22Receive] EB22 파일 없음 또는 수신 실패 (불능 0건일 수 있음): {}", e.getMessage());
            markAllSuccess(targetDate);
            return;
        }

        // 2. NCP 백업 업로드
        try (ByteArrayInputStream bis = new ByteArrayInputStream(fileBytes)) {
            String objectKey = "cms/result/" + eb22FileName;
            storageService.upload(objectKey, bis, fileBytes.length, "text/plain");

            sqlRunner.execute(/* skip_tenant_check */
                    """
                    INSERT INTO cms_eb_file (
                        spjangcd, file_name, file_type, file_path,
                        target_date, billing_count, billing_amount,
                        send_status, _creater_id, _created, _modifier_id, _modified
                    ) VALUES (
                        'SYSTEM', :fileName, 'RESULT', :filePath,
                        CAST(:targetDate AS DATE), 0, 0,
                        'RECEIVED', 'SYSTEM', NOW(), 'SYSTEM', NOW()
                    )
                    """,
                    new MapSqlParameterSource()
                            .addValue("fileName",   eb22FileName)
                            .addValue("filePath",   objectKey)
                            .addValue("targetDate", targetDate));
        } catch (Exception e) {
            log.warn("[CmsEb22Receive] NCP 업로드 실패 (처리는 계속): {}", e.getMessage());
        }

        // 3. EB22 파싱 — 불능 납부자번호(member_no) + 불능코드 추출
        Map<String, String> failMap = parseEb22(fileBytes); // key=member_no, value=result_code
        log.info("[CmsEb22Receive] 불능 건수: {}", failMap.size());

        // 4. 어제 출금일 REQUESTED 상태 billing 전체 조회
        List<Map<String, Object>> requestedBillings = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT b.id, m.member_no
                FROM cms_billing b
                LEFT JOIN cms_member m ON m.id = b.member_id
                WHERE b.deduct_date = :targetDate
                  AND b.status      = 'REQUESTED'
                """,
                new MapSqlParameterSource("targetDate", targetDate));

        int successCount = 0, failCount = 0;

        for (Map<String, Object> b : requestedBillings) {
            long   billingId = ((Number) b.get("id")).longValue();
            String memberNo  = str(b.get("member_no"));
            var    p         = new MapSqlParameterSource("billingId", billingId);
            p.addValue("resultDate", targetDate );

            // FAIL → result_date null
            if (failMap.containsKey(memberNo)) {
                String resultCode = failMap.get(memberNo);
                p.addValue("resultCode", resultCode);
                p.addValue("resultMsg",  resolveFailMsg(resultCode));
                sqlRunner.execute(/* skip_tenant_check */
                        """
                        UPDATE cms_billing SET
                            status      = 'FAIL',
                            result_code = :resultCode,
                            result_msg  = :resultMsg,
                            result_date = NULL,
                            _modified   = NOW()
                        WHERE id = :billingId AND status = 'REQUESTED'
                        """, p);
                failCount++;
            } else {
                sqlRunner.execute(/* skip_tenant_check */
                        """
                        UPDATE cms_billing SET
                            status      = 'SUCCESS',
                            result_code = '0000',
                            result_msg  = '출금성공',
                            result_date = :resultDate,
                            _modified   = NOW()
                        WHERE id = :billingId AND status = 'REQUESTED'
                        """, p);
                successCount++;
            }
        }

        log.info("[CmsEb22Receive] 완료 - 성공: {}건, 실패: {}건", successCount, failCount);
    }

    /** EB22 파일이 없을 때 — 전체 REQUESTED → SUCCESS */
    private void markAllSuccess(String targetDate) {
        String today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        var p = new MapSqlParameterSource();
        p.addValue("targetDate", targetDate);
        p.addValue("resultDate", today);
        int cnt = sqlRunner.execute(/* skip_tenant_check */
                """
                UPDATE cms_billing SET
                    status      = 'SUCCESS',
                    result_code = '0000',
                    result_msg  = '출금성공',
                    result_date = :resultDate,
                    _modified   = NOW()
                WHERE deduct_date = :targetDate AND status = 'REQUESTED'
                """, p);
        log.info("[CmsEb22Receive] EB22 없음 → 전체 SUCCESS 처리 {}건", cnt);
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

    private byte[] sftpDownloadWithApiCredential(String fileName, String targetDate) throws Exception {
        String[] cred = cmsEbFileGenerateService.getSftpReceiveCredential("EB22", targetDate);
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
