package mes.app.cms;

import mes.app.cms.service.CmsEbFileService;
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
public class CmsEbFileController {

    @Autowired
    private CmsEbFileService cmsEbFileService;

    // ── EB 파일 (익일출금) ────────────────────────────────────────────────────

    /** 목록 조회 */
    @GetMapping("/eb-files")
    public AjaxResult getList(
            @RequestParam(value = "date_from",   required = false) String dateFrom,
            @RequestParam(value = "date_to",     required = false) String dateTo,
            @RequestParam(value = "file_type",   required = false) String fileType,
            @RequestParam(value = "send_status", required = false) String sendStatus,
            HttpServletRequest request) {

        List<Map<String, Object>> items = cmsEbFileService.getEbFileList(dateFrom, dateTo, fileType, sendStatus);
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
        Map<String, Object> res = cmsEbFileService.generateEbFile(targetDate, user.getUsername());
        AjaxResult result = new AjaxResult();
        if (res.containsKey("error")) { result.success = false; result.message = (String) res.get("error"); }
        else { result.data = res; }
        return result;
    }

    /** 파일 다운로드 */
    @GetMapping("/eb-files/{id}/download")
    public void download(@PathVariable Long id, HttpServletResponse response) throws Exception {
        cmsEbFileService.downloadEbFile(id, response);
    }

    /** SFTP 수동 전송 */
    @PostMapping("/eb-files/{id}/send-sftp")
    public AjaxResult sendSftp(@PathVariable Long id, Authentication auth) {
        User user = (User) auth.getPrincipal();
        AjaxResult result = new AjaxResult();
        boolean ok = cmsEbFileService.sendSftp(id, user.getUsername());
        if (!ok) { result.success = false; result.message = "SFTP 전송에 실패했습니다."; }
        return result;
    }

    /** 삭제 (PENDING/FAILED 상태만) */
    @DeleteMapping("/eb-files/{id}")
    public AjaxResult delete(@PathVariable Long id) {
        AjaxResult result = new AjaxResult();
        boolean ok = cmsEbFileService.deleteEbFile(id);
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
        List<Map<String, Object>> items = cmsEbFileService.getEcFileList(dateFrom, dateTo, fileType, sendStatus);
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
        Map<String, Object> res = cmsEbFileService.generateEcFile(targetDate, user.getUsername());
        AjaxResult result = new AjaxResult();
        if (res.containsKey("error")) { result.success = false; result.message = (String) res.get("error"); }
        else { result.data = res; }
        return result;
    }

    /** 파일 다운로드 */
    @GetMapping("/ec-files/{id}/download")
    public void downloadEc(@PathVariable Long id, HttpServletResponse response) throws Exception {
        cmsEbFileService.downloadEcFile(id, response);
    }

    /** SFTP 수동 전송 */
    @PostMapping("/ec-files/{id}/send-sftp")
    public AjaxResult sendEcSftp(@PathVariable Long id, Authentication auth) {
        User user = (User) auth.getPrincipal();
        AjaxResult result = new AjaxResult();
        boolean ok = cmsEbFileService.sendEcSftp(id, user.getUsername());
        if (!ok) { result.success = false; result.message = "SFTP 전송에 실패했습니다."; }
        return result;
    }

    /** 삭제 (PENDING/FAILED 상태만) */
    @DeleteMapping("/ec-files/{id}")
    public AjaxResult deleteEc(@PathVariable Long id) {
        AjaxResult result = new AjaxResult();
        boolean ok = cmsEbFileService.deleteEcFile(id);
        if (!ok) { result.success = false; result.message = "삭제 실패 — PENDING 상태인 파일만 삭제할 수 있습니다."; }
        return result;
    }
}
