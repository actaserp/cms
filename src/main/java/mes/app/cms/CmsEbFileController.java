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
@RequestMapping("/api/cms/eb-files")
public class CmsEbFileController {

    @Autowired
    private CmsEbFileService cmsEbFileService;

    /** 목록 조회 */
    @GetMapping
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
    @PostMapping("/generate")
    public AjaxResult generate(
            @RequestParam("target_date") String targetDate,
            Authentication auth) {

        User user = (User) auth.getPrincipal();
        Map<String, Object> res = cmsEbFileService.generateEbFile(targetDate, user.getUsername());
        AjaxResult result = new AjaxResult();
        if (res.containsKey("error")) {
            result.success = false;
            result.message = (String) res.get("error");
        } else {
            result.data = res;
        }
        return result;
    }

    /** 파일 다운로드 */
    @GetMapping("/{id}/download")
    public void download(@PathVariable Long id, HttpServletResponse response) throws Exception {
        cmsEbFileService.downloadEbFile(id, response);
    }

    /** SFTP 수동 전송 */
    @PostMapping("/{id}/send-sftp")
    public AjaxResult sendSftp(@PathVariable Long id, Authentication auth) {
        User user = (User) auth.getPrincipal();
        AjaxResult result = new AjaxResult();
        boolean ok = cmsEbFileService.sendSftp(id, user.getUsername());
        if (!ok) {
            result.success = false;
            result.message = "SFTP 전송에 실패했습니다.";
        }
        return result;
    }

    /** 삭제 (PENDING 상태만) */
    @DeleteMapping("/{id}")
    public AjaxResult delete(@PathVariable Long id) {
        AjaxResult result = new AjaxResult();
        boolean ok = cmsEbFileService.deleteEbFile(id);
        if (!ok) {
            result.success = false;
            result.message = "삭제 실패 — PENDING 상태인 파일만 삭제할 수 있습니다.";
        }
        return result;
    }
}
