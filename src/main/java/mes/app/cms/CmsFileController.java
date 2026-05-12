package mes.app.cms;

import mes.app.cms.service.CmsFileService;
import mes.app.cms.service.CmsTokenService;
import mes.app.common.TenantContext;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/cms")
public class CmsFileController {

    @Autowired
    private CmsFileService cmsFileService;

    @Autowired
    private CmsTokenService cmsTokenService;


    // ── EB 파일 (익일출금) ────────────────────────────────────────────────────

    /** 목록 조회 */
    @GetMapping("/eb-files")
    public AjaxResult getList(
            @RequestParam(value = "date_from",   required = false) String dateFrom,
            @RequestParam(value = "date_to",     required = false) String dateTo,
            @RequestParam(value = "file_type",   required = false) String fileType,
            @RequestParam(value = "send_status", required = false) String sendStatus,
            HttpServletRequest request) {
        if (!org.springframework.util.StringUtils.hasText(fileType)) fileType = "EB_REQUEST";
        List<Map<String, Object>> items = cmsFileService.getEbFileList(dateFrom, dateTo, fileType, sendStatus);
        AjaxResult result = new AjaxResult();
        result.data = items;
        return result;
    }

    /** 수동 생성 */
    @PostMapping("/eb-files/generate")
    public AjaxResult generate(
            @RequestParam("target_date") String targetDate,
            Authentication auth) {

        User user = (User) auth.getPrincipal();
        Map<String, Object> res = cmsFileService.generateEbFile(targetDate, user.getUsername());
        AjaxResult result = new AjaxResult();
        if (res.containsKey("error")) { result.success = false; result.message = (String) res.get("error"); }
        else { result.data = res; }
        return result;
    }

    /** 파일 다운로드 */
    @GetMapping("/eb-files/{id}/download")
    public void download(@PathVariable Long id, HttpServletResponse response) throws Exception {
        cmsFileService.downloadEbFile(id, response);
    }

    /** SFTP 수동 전송 */
    @PostMapping("/eb-files/{id}/send-sftp")
    public AjaxResult sendSftp(@PathVariable Long id, Authentication auth) {
        User user = (User) auth.getPrincipal();
        AjaxResult result = new AjaxResult();
        boolean ok = cmsFileService.sendSftp(id, user.getUsername());
        if (!ok) { result.success = false; result.message = "SFTP 전송에 실패했습니다."; }
        return result;
    }

    /** 삭제 (PENDING/FAILED 상태만) */
    @DeleteMapping("/eb-files/{id}")
    public AjaxResult delete(@PathVariable Long id) {
        AjaxResult result = new AjaxResult();
        boolean ok = cmsFileService.deleteEbFile(id);
        if (!ok) { result.success = false; result.message = "삭제 실패 — PENDING 상태인 파일만 삭제할 수 있습니다."; }
        return result;
    }

    // ── EC 파일 (당일출금) ────────────────────────────────────────────────────

    /** 목록 조회 */
    @GetMapping("/ec-files")
    public AjaxResult getEcList(
            @RequestParam(value = "date_from",   required = false) String dateFrom,
            @RequestParam(value = "date_to",     required = false) String dateTo,
            @RequestParam(value = "file_type",   required = false) String fileType,
            @RequestParam(value = "send_status", required = false) String sendStatus,
            HttpServletRequest request) {

        // EC 목록만 조회 — file_type 미지정 시 EC% 필터 적용
        if (!org.springframework.util.StringUtils.hasText(fileType)) fileType = "EC_REQUEST";
        List<Map<String, Object>> items = cmsFileService.getEcFileList(dateFrom, dateTo, fileType, sendStatus);
        AjaxResult result = new AjaxResult();
        result.data = items;
        return result;
    }

    /** 수동 생성 */
    @PostMapping("/ec-files/generate")
    public AjaxResult generateEc(
            @RequestParam("target_date") String targetDate,
            Authentication auth) {

        User user = (User) auth.getPrincipal();
        Map<String, Object> res = cmsFileService.generateEcFile(targetDate, user.getUsername());
        AjaxResult result = new AjaxResult();
        if (res.containsKey("error")) { result.success = false; result.message = (String) res.get("error"); }
        else { result.data = res; }
        return result;
    }

    /** 파일 다운로드 */
    @GetMapping("/ec-files/{id}/download")
    public void downloadEc(@PathVariable Long id, HttpServletResponse response) throws Exception {
        cmsFileService.downloadEcFile(id, response);
    }

    /** SFTP 수동 전송 */
    @PostMapping("/ec-files/{id}/send-sftp")
    public AjaxResult sendEcSftp(@PathVariable Long id, Authentication auth) {
        User user = (User) auth.getPrincipal();
        AjaxResult result = new AjaxResult();
        boolean ok = cmsFileService.sendEcSftp(id, user.getUsername());
        if (!ok) { result.success = false; result.message = "SFTP 전송에 실패했습니다."; }
        return result;
    }

    /** 삭제 (PENDING/FAILED 상태만) */
    @DeleteMapping("/ec-files/{id}")
    public AjaxResult deleteEc(@PathVariable Long id) {
        AjaxResult result = new AjaxResult();
        boolean ok = cmsFileService.deleteEcFile(id);
        if (!ok) { result.success = false; result.message = "삭제 실패 — PENDING 상태인 파일만 삭제할 수 있습니다."; }
        return result;
    }

    @PostMapping("/cms-files/{id}/status")
    public AjaxResult checkFileStatus(@PathVariable Long id) {
        AjaxResult result = new AjaxResult();
        try {
            Map<String, Object> file = cmsFileService.getFile(id);
            if (file == null) {
                result.success = false; result.message = "파일을 찾을 수 없습니다."; return result;
            }
            String spjangcd   = String.valueOf(file.get("spjangcd"));
            String fileName   = String.valueOf(file.get("file_name"));
            String fileType   = fileName.substring(0, 4);
            String targetDate = String.valueOf(file.get("target_date")).replace("-", "");
            com.fasterxml.jackson.databind.JsonNode statusData = cmsTokenService.getFileStatus(spjangcd, fileType, targetDate);
            result.data = statusData.path("data");
        } catch (Exception e) {
            result.success = false; result.message = e.getMessage();
        }
        return result;
    }

    @PostMapping("/cms-files/{id}/cancel")
    public AjaxResult cancelCmsFile(@PathVariable Long id) {
        AjaxResult result = new AjaxResult();
        try {
            Map<String, Object> file = cmsFileService.getFile(id);
            if (file == null) {
                result.success = false; result.message = "파일을 찾을 수 없습니다."; return result;
            }
            if (!"SENT".equals(file.get("send_status"))) {
                result.success = false; result.message = "전송완료 상태인 파일만 취소할 수 있습니다."; return result;
            }

            String spjangcd   = String.valueOf(file.get("spjangcd"));
            String fileName   = String.valueOf(file.get("file_name"));
            String fileType   = fileName.substring(0, 4);
            String targetDate = String.valueOf(file.get("target_date")).replace("-", "");
            cmsTokenService.getFileStatus(spjangcd, fileType, targetDate);
            boolean cancelled = cmsTokenService.cancelFile(spjangcd, fileType, targetDate);
            if (cancelled) {
                cmsFileService.updateSendStatus(id, "CANCELLED");
            } else {
                result.success = false; result.message = "금결원 취소 요청 실패";
            }
        } catch (Exception e) {
            result.success = false; result.message = e.getMessage();
        }
        return result;
    }
}