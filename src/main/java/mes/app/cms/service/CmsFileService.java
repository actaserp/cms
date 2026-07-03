package mes.app.cms.service;

import lombok.extern.slf4j.Slf4j;
import mes.app.Scheduler.SchedulerService.CmsEb21SendService;
import mes.app.Scheduler.SchedulerService.CmsEc21SendService;
import mes.app.common.TenantContext;
import mes.app.files.NcpObjectStorageService;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedOutputStream;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class CmsFileService {

    @Autowired SqlRunner sqlRunner;
    @Autowired NcpObjectStorageService storageService;
    @Autowired
    CmsEb21SendService cmsEb21SendService;
    @Autowired
    CmsEc21SendService cmsEc21SendService;

    // ── 목록 조회 ──────────────────────────────────────────────────────────────

    public Map<String, Object> getCmsFileList(String dateFrom, String dateTo,
                                               String fileType, String sendStatus,
                                               int page, int size) {
        String spjangcd = TenantContext.get();
        var param = new MapSqlParameterSource("spjangcd", spjangcd);

        String baseSelect =
            "SELECT f.id, f.spjangcd, f.file_name, f.file_type, f.target_date," +
            "       f.billing_count, f.billing_amount, f.send_type," +
            "       f.send_status, f.sent_at, f.error_message, f._creater_id," +
            "       TO_CHAR(f._created AT TIME ZONE 'Asia/Seoul', 'YYYY-MM-DD HH24:MI:SS') AS _created," +
            "       TO_CHAR(f.sent_at  AT TIME ZONE 'Asia/Seoul', 'YYYY-MM-DD HH24:MI:SS') AS sent_at_kst," +
            "       COALESCE(" +
            "           (SELECT b.member_name FROM cms_file_billing fb" +
            "            JOIN cms_billing b ON b.id = fb.billing_id" +
            "            WHERE fb.file_id = f.id ORDER BY fb.line_seq LIMIT 1)," +
            "           (SELECT b.member_name FROM cms_file rf" +
            "            JOIN cms_file_billing fb ON fb.file_id = rf.id" +
            "            JOIN cms_billing b ON b.id = fb.billing_id" +
            "            WHERE rf.spjangcd = f.spjangcd AND rf.target_date = f.target_date" +
            "              AND rf.file_type = CASE f.file_type" +
            "                  WHEN 'EB22' THEN 'EB21' WHEN 'EC22' THEN 'EC21'" +
            "                  WHEN 'EB14' THEN 'EB13' ELSE NULL END" +
            "            ORDER BY rf.id DESC, fb.line_seq LIMIT 1)" +
            "       ) AS rep_member_name" +
            "  FROM cms_file f" +
            "  WHERE f.spjangcd = :spjangcd";

        String filters = "";
        if (StringUtils.hasText(dateFrom))  { filters += " AND f.target_date >= CAST(:dateFrom AS DATE)"; param.addValue("dateFrom", dateFrom); }
        if (StringUtils.hasText(dateTo))    { filters += " AND f.target_date <= CAST(:dateTo AS DATE)";   param.addValue("dateTo", dateTo); }
        if (StringUtils.hasText(fileType))  { filters += " AND f.file_type = :fileType";                  param.addValue("fileType", fileType); }
        if (StringUtils.hasText(sendStatus)){ filters += " AND f.send_status = :sendStatus";              param.addValue("sendStatus", sendStatus); }

        // 전체 건수 + 청구금액 합계
        Map<String, Object> aggRow = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT COUNT(*) AS cnt, COALESCE(SUM(f.billing_amount),0) AS total_amount" +
                "  FROM cms_file f WHERE f.spjangcd = :spjangcd" + filters, param);
        long totalCount  = aggRow != null ? ((Number) aggRow.get("cnt")).longValue()          : 0L;
        long totalAmount = aggRow != null ? ((Number) aggRow.get("total_amount")).longValue() : 0L;

        param.addValue("pgSize",   size);
        param.addValue("pgOffset", (long) page * size);
        List<Map<String, Object>> rows = sqlRunner.getRows(
                baseSelect + filters + " ORDER BY COALESCE(f._modified, f._created) DESC LIMIT :pgSize OFFSET :pgOffset", param);

        return Map.of("data", rows, "totalCount", totalCount, "totalAmount", totalAmount);
    }

    public List<Map<String, Object>> getCmsFileBillings(Long fileId) {
        // 직접 매핑 건수 확인
        Map<String, Object> countRow = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT COUNT(*) AS cnt FROM cms_file_billing WHERE file_id = :fileId",
                new MapSqlParameterSource("fileId", fileId));
        long cnt = countRow != null ? ((Number) countRow.get("cnt")).longValue() : 0;

        Long targetFileId = fileId;
        if (cnt == 0) {
            // 결과 파일이면 연결된 요청 파일 찾기
            Map<String, Object> reqFile = sqlRunner.getRow(/* skip_tenant_check */
                    """
                    SELECT req.id FROM cms_file f
                    JOIN cms_file req ON req.spjangcd = f.spjangcd
                        AND req.target_date = f.target_date
                        AND req.file_type = CASE f.file_type
                            WHEN 'EB22' THEN 'EB21'
                            WHEN 'EC22' THEN 'EC21'
                            WHEN 'EB14' THEN 'EB13'
                            ELSE NULL
                        END
                    WHERE f.id = :fileId
                    ORDER BY req.id DESC
                    LIMIT 1
                    """,
                    new MapSqlParameterSource("fileId", fileId));
            if (reqFile != null && reqFile.get("id") != null) {
                targetFileId = ((Number) reqFile.get("id")).longValue();
            }
        }

        return sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT fb.line_seq, b.member_name, bc.bank_name, b.bank_account,
                       b.billing_amount, b.status
                FROM cms_file_billing fb
                JOIN cms_billing b ON b.id = fb.billing_id
                LEFT JOIN cms_bank_code bc ON bc.bank_code = b.bank_code
                WHERE fb.file_id = :fileId
                ORDER BY fb.line_seq
                """,
                new MapSqlParameterSource("fileId", targetFileId));
    }

    // ── 수동 생성 (화면) — GenerateService 위임 ───────────────────────────────

    public Map<String, Object> generateEbFile(String targetDate, String userId) {
        return cmsEb21SendService.runForSpjang(TenantContext.get(), targetDate, userId);
    }

    // ── 다운로드 ───────────────────────────────────────────────────────────────


    // ── 수동 SFTP 재전송 (FAILED 파일 재시도) ─────────────────────────────────

    public boolean sendSftp(Long id, String userId) {
        String spjangcd = TenantContext.get();
        var param = new MapSqlParameterSource("id", id).addValue("spjangcd", spjangcd);

        Map<String, Object> row = sqlRunner.getRow(
                "SELECT send_status, file_name, file_path, TO_CHAR(target_date,'YYYYMMDD') AS target_date_str FROM cms_file WHERE id = :id AND spjangcd = :spjangcd", param);

        if (row == null) return false;
        String sendStatus = (String) row.get("send_status");
        if (!"PENDING".equals(sendStatus) && !"FAILED".equals(sendStatus)) return false;

        try {
            String objectKey  = (String) row.get("file_path");
            String fileName   = (String) row.get("file_name");
            String targetDate = (String) row.get("target_date_str");

            // NCP에서 파일 읽어 SFTP 전송
            try (ResponseInputStream<GetObjectResponse> s3Stream = storageService.download(objectKey)) {
                byte[] fileBytes = s3Stream.readAllBytes();
                String fileType = ((String) row.get("file_name")).substring(0, 4);
                if (fileType.startsWith("EC")) {
                    cmsEc21SendService.sftpSendBytes(fileBytes, fileName, targetDate, spjangcd);
                } else {
                    cmsEb21SendService.sftpSendBytes(fileBytes, fileName, targetDate, spjangcd);
                }
            }

            var up = new MapSqlParameterSource("id", id).addValue("userId", userId);
            sqlRunner.execute("""
                    UPDATE cms_file SET send_status='SENT', send_type='SFTP', sent_at=NOW(),
                        _modifier_id=:userId, _modified=NOW()
                    WHERE id=:id
                    """, up);
            return true;
        } catch (Exception e) {
            log.error("SFTP 수동 전송 오류", e);
            var ep = new MapSqlParameterSource("id", id).addValue("errMsg", e.getMessage()).addValue("userId", userId);
            sqlRunner.execute("""
                    UPDATE cms_file SET send_status='FAILED', error_message=:errMsg,
                        _modifier_id=:userId, _modified=NOW()
                    WHERE id=:id
                    """, ep);
            return false;
        }
    }

    public void downloadFile(Long id, HttpServletResponse response) throws Exception {
        String spjangcd = TenantContext.get();
        var param = new MapSqlParameterSource("id", id).addValue("spjangcd", spjangcd);
        Map<String, Object> row = sqlRunner.getRow(
                "SELECT file_name, file_path FROM cms_file WHERE id = :id AND spjangcd = :spjangcd", param);

        if (row == null) { response.sendError(HttpServletResponse.SC_NOT_FOUND); return; }

        String objectKey = (String) row.get("file_path");
        String fileName  = (String) row.get("file_name");

        try (ResponseInputStream<GetObjectResponse> s3Stream = storageService.download(objectKey);
             BufferedOutputStream out = new BufferedOutputStream(response.getOutputStream())) {
            String encoded = URLEncoder.encode(fileName, "UTF-8").replace("+", "%20");
            response.setContentType("application/octet-stream");
            response.setHeader("Content-Disposition", "attachment; filename*=UTF-8''" + encoded);
            byte[] buf = new byte[8192];
            int read;
            while ((read = s3Stream.read(buf)) != -1) out.write(buf, 0, read);
            out.flush();
        } catch (Exception e) {
            log.error("파일 다운로드 오류: {}", objectKey, e);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "다운로드 오류");
        }
    }

    public Map<String, Object> generateEcFile(String targetDate, String userId) {
        return cmsEc21SendService.runForSpjang(TenantContext.get(), targetDate, userId);
    }

    // ── 삭제 (PENDING / FAILED 상태만) ────────────────────────────────────────

    @Transactional
    public boolean deleteFile(Long id) {
        String spjangcd = TenantContext.get();
        var param = new MapSqlParameterSource("id", id).addValue("spjangcd", spjangcd);

        Map<String, Object> row = sqlRunner.getRow(
                "SELECT file_path, send_status, file_type FROM cms_file WHERE id = :id AND spjangcd = :spjangcd", param);

        if (row == null) return false;
        String status = (String) row.get("send_status");
        if (!"PENDING".equals(status) && !"FAILED".equals(status)) return false;

        // billing 상태 롤백 (REQUESTED → PENDING)
        sqlRunner.execute("UPDATE cms_billing SET status='PENDING', file_id=NULL, _modified=NOW() WHERE file_id=:id AND spjangcd=:spjangcd", param);
        sqlRunner.execute("DELETE FROM cms_file_billing WHERE file_id=:id", param);

        String objectKey = (String) row.get("file_path");
        if (StringUtils.hasText(objectKey)) {
            try { storageService.delete(objectKey); } catch (Exception e) { log.warn("NCP 파일 삭제 실패: {}", objectKey); }
        }

        String fileType2 = (String) row.get("file_type");

        if ("EI13".equals(fileType2) || "EB13".equals(fileType2)) {
            sqlRunner.execute("""
        UPDATE cms_account_register r
        SET ei13_status = CASE WHEN :isEi13 THEN 'PENDING' ELSE ei13_status END,
            eb13_status = CASE WHEN :isEb13 THEN 'PENDING' ELSE eb13_status END,
            status = 'PENDING', _modified = NOW()
        FROM cms_file_register fr
        WHERE fr.file_id = :id AND fr.register_id = r.id
        """,
                    param.addValue("isEi13", "EI13".equals(fileType2))
                            .addValue("isEb13", "EB13".equals(fileType2)));
            sqlRunner.execute("DELETE FROM cms_file_register WHERE file_id=:id", param);
        }

        return sqlRunner.execute("DELETE FROM cms_file WHERE id=:id AND spjangcd=:spjangcd", param) > 0;
    }

    public Map<String, Object> getFile(Long id) {
        var param = new MapSqlParameterSource("id", id);
        return sqlRunner.getRow(
                "SELECT id, spjangcd, file_name, file_type, send_status, TO_CHAR(target_date,'YYYYMMDD') AS target_date FROM cms_file WHERE id=:id",
                param);
    }

    public void updateSendStatus(Long id, String sendStatus) {
        var param = new MapSqlParameterSource("id", id)
                .addValue("sendStatus", sendStatus);
        sqlRunner.execute(
                "UPDATE cms_file SET send_status=:sendStatus, _modified=NOW() WHERE id=:id",
                param);
    }

    public void revertBillingsToPending(Long fileId) {
        var param = new MapSqlParameterSource("fileId", fileId);
        sqlRunner.execute("""
        UPDATE cms_billing SET status='PENDING', result_code=NULL,
            result_msg=NULL, result_date=NULL, _modified=NOW(), memo = '파일 취소로 인한 재대기'
        WHERE file_id = :fileId AND status = 'REQUESTED'
        """, param);

        sqlRunner.execute(/* skip_tenant_check */
                "DELETE FROM cms_file_billing WHERE file_id = :fileId",
                new MapSqlParameterSource("fileId", fileId));


    }

    public boolean hasResultFile(Long fileId) {
        Map<String, Object> file = getFile(fileId);
        if (file == null) return false;
        String targetDate = String.valueOf(file.get("target_date"));
        String spjangcd = String.valueOf(file.get("spjangcd"));

        Map<String, String> resultTypeMap = Map.of(
                "EB21", "EB22",
                "EC21", "EC22",
                "EB13", "EB14"
        );
        String requestType = String.valueOf(file.get("file_name")).substring(0, 4);
        String resultType  = resultTypeMap.getOrDefault(requestType, null);
        if (resultType == null) return false;

        List<Map<String, Object>> resultFiles = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT 1 FROM cms_file
                WHERE spjangcd = :spjangcd
                  AND file_type = :resultType
                  AND target_date = CAST(:targetDate AS DATE)
                """,
                new MapSqlParameterSource()
                        .addValue("spjangcd",   spjangcd)
                        .addValue("resultType", resultType)
                        .addValue("targetDate", targetDate));
        return !resultFiles.isEmpty();
    }

    public void revertRegistersToPending(Long fileId) {
        Map<String, Object> file = getFile(fileId);
        if (file == null) return;
        String fileType = (String) file.get("file_type");
        if (!"EI13".equals(fileType) && !"EB13".equals(fileType)) return;

        var param = new MapSqlParameterSource("fileId", fileId);
        sqlRunner.execute("""
        UPDATE cms_account_register r
        SET ei13_status = CASE WHEN :isEi13 THEN 'PENDING' ELSE ei13_status END,
            eb13_status = CASE WHEN :isEb13 THEN 'PENDING' ELSE eb13_status END,
            status = 'PENDING', _modified = NOW()
        FROM cms_file_register fr
        WHERE fr.file_id = :fileId AND fr.register_id = r.id
        """,
                param.addValue("isEi13", "EI13".equals(fileType))
                        .addValue("isEb13", "EB13".equals(fileType)));
        sqlRunner.execute("DELETE FROM cms_file_register WHERE file_id=:fileId", param);
    }

    /** 센터오류 상세에 납부자명 매핑 (payer_no = cms_member.member_no) */
    public void attachMemberNames(String spjangcd, List<Map<String, Object>> details) {
        if (details == null) return;
        for (Map<String, Object> d : details) {
            Object payerNo = d.get("payerNo");
            if (payerNo == null || !StringUtils.hasText(payerNo.toString())) continue;
            Map<String, Object> mem = sqlRunner.getRow(/* skip_tenant_check */
                    "SELECT member_name FROM cms_member WHERE spjangcd = :spjangcd AND member_no = :memberNo",
                    new MapSqlParameterSource("spjangcd", spjangcd).addValue("memberNo", payerNo.toString()));
            d.put("memberName", mem != null ? mem.get("member_name") : "");
        }
    }
}
