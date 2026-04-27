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
public class CmsEbFileService {

    @Autowired SqlRunner sqlRunner;
    @Autowired NcpObjectStorageService storageService;
    @Autowired
    CmsEb21SendService cmsEb21SendService;
    @Autowired
    CmsEc21SendService cmsEc21SendService;

    // ── 목록 조회 ──────────────────────────────────────────────────────────────

    public List<Map<String, Object>> getEbFileList(String dateFrom, String dateTo,
                                                    String fileType, String sendStatus) {
        String spjangcd = TenantContext.get();
        var param = new MapSqlParameterSource("spjangcd", spjangcd);

        String sql = """
                SELECT f.id, f.file_name, f.file_type, f.target_date,
                       f.billing_count, f.billing_amount, f.send_type,
                       f.send_status, f.sent_at, f.error_message,
                       f._creater_id, f._created
                FROM cms_file f
                WHERE f.spjangcd = :spjangcd
                """;

        if (StringUtils.hasText(dateFrom)) { sql += " AND f.target_date >= CAST(:dateFrom AS DATE)"; param.addValue("dateFrom", dateFrom); }
        if (StringUtils.hasText(dateTo))   { sql += " AND f.target_date <= CAST(:dateTo AS DATE)";   param.addValue("dateTo", dateTo); }
        if (StringUtils.hasText(fileType)) { sql += " AND f.file_type = :fileType";                  param.addValue("fileType", fileType); }
        if (StringUtils.hasText(sendStatus)){ sql += " AND f.send_status = :sendStatus";             param.addValue("sendStatus", sendStatus); }

        return sqlRunner.getRows(sql + " ORDER BY f._created DESC", param);
    }

    // ── 수동 생성 (화면) — GenerateService 위임 ───────────────────────────────

    public Map<String, Object> generateEbFile(String targetDate, String userId) {
        return cmsEb21SendService.runForSpjang(TenantContext.get(), targetDate, userId);
    }

    // ── 다운로드 ───────────────────────────────────────────────────────────────

    public void downloadEbFile(Long id, HttpServletResponse response) throws Exception {
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
            log.error("EB파일 다운로드 오류: {}", objectKey, e);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "다운로드 오류");
        }
    }

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
                cmsEb21SendService.sftpSendBytes(fileBytes, fileName, targetDate);
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

    // ── EC 파일 (당일출금) ─────────────────────────────────────────────────────

    public List<Map<String, Object>> getEcFileList(String dateFrom, String dateTo,
                                                    String fileType, String sendStatus) {
        String spjangcd = TenantContext.get();
        var param = new MapSqlParameterSource("spjangcd", spjangcd);

        String sql = """
                SELECT f.id, f.file_name, f.file_type, f.target_date,
                       f.billing_count, f.billing_amount, f.send_type,
                       f.send_status, f.sent_at, f.error_message,
                       f._creater_id, f._created
                FROM cms_file f
                WHERE f.spjangcd = :spjangcd
                """;

        if (StringUtils.hasText(dateFrom))  { sql += " AND f.target_date >= CAST(:dateFrom AS DATE)"; param.addValue("dateFrom", dateFrom); }
        if (StringUtils.hasText(dateTo))    { sql += " AND f.target_date <= CAST(:dateTo AS DATE)";   param.addValue("dateTo", dateTo); }
        if (StringUtils.hasText(fileType))  { sql += " AND f.file_type = :fileType";                  param.addValue("fileType", fileType); }
        if (StringUtils.hasText(sendStatus)){ sql += " AND f.send_status = :sendStatus";              param.addValue("sendStatus", sendStatus); }

        return sqlRunner.getRows(sql + " ORDER BY f._created DESC", param);
    }

    public Map<String, Object> generateEcFile(String targetDate, String userId) {
        return cmsEc21SendService.runForSpjang(TenantContext.get(), targetDate, userId);
    }

    public void downloadEcFile(Long id, HttpServletResponse response) throws Exception {
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
            log.error("EC파일 다운로드 오류: {}", objectKey, e);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "다운로드 오류");
        }
    }

    public boolean sendEcSftp(Long id, String userId) {
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

            try (ResponseInputStream<GetObjectResponse> s3Stream = storageService.download(objectKey)) {
                byte[] fileBytes = s3Stream.readAllBytes();
                cmsEc21SendService.sftpSendBytes(fileBytes, fileName, targetDate);
            }

            var up = new MapSqlParameterSource("id", id).addValue("userId", userId);
            sqlRunner.execute("""
                    UPDATE cms_file SET send_status='SENT', send_type='SFTP', sent_at=NOW(),
                        _modifier_id=:userId, _modified=NOW()
                    WHERE id=:id
                    """, up);
            return true;
        } catch (Exception e) {
            log.error("EC SFTP 수동 전송 오류", e);
            var ep = new MapSqlParameterSource("id", id).addValue("errMsg", e.getMessage()).addValue("userId", userId);
            sqlRunner.execute("""
                    UPDATE cms_file SET send_status='FAILED', error_message=:errMsg,
                        _modifier_id=:userId, _modified=NOW()
                    WHERE id=:id
                    """, ep);
            return false;
        }
    }

    @Transactional
    public boolean deleteEcFile(Long id) {
        String spjangcd = TenantContext.get();
        var param = new MapSqlParameterSource("id", id).addValue("spjangcd", spjangcd);

        Map<String, Object> row = sqlRunner.getRow(
                "SELECT file_path, send_status FROM cms_file WHERE id = :id AND spjangcd = :spjangcd", param);

        if (row == null) return false;
        String status = (String) row.get("send_status");
        if (!"PENDING".equals(status) && !"FAILED".equals(status)) return false;

        sqlRunner.execute("UPDATE cms_billing SET status='PENDING', file_id=NULL, _modified=NOW() WHERE file_id=:id AND spjangcd=:spjangcd", param);
        sqlRunner.execute("DELETE FROM cms_file_billing WHERE file_id=:id", param);

        String objectKey = (String) row.get("file_path");
        if (StringUtils.hasText(objectKey)) {
            try { storageService.delete(objectKey); } catch (Exception e) { log.warn("NCP 파일 삭제 실패: {}", objectKey); }
        }

        return sqlRunner.execute("DELETE FROM cms_file WHERE id=:id AND spjangcd=:spjangcd", param) > 0;
    }

    // ── 삭제 (PENDING / FAILED 상태만) ────────────────────────────────────────

    @Transactional
    public boolean deleteEbFile(Long id) {
        String spjangcd = TenantContext.get();
        var param = new MapSqlParameterSource("id", id).addValue("spjangcd", spjangcd);

        Map<String, Object> row = sqlRunner.getRow(
                "SELECT file_path, send_status FROM cms_file WHERE id = :id AND spjangcd = :spjangcd", param);

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

        return sqlRunner.execute("DELETE FROM cms_file WHERE id=:id AND spjangcd=:spjangcd", param) > 0;
    }
}
