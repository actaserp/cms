package mes.app.cms;

import mes.app.cms.service.CmsSignService;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Controller
public class CmsSignController {

    @Autowired
    private CmsSignService cmsSignService;

    /** 서명 페이지 (로그인 불필요) */
    @GetMapping("/agree/{token}")
    public String signPage() {
        return "cms/cms_sign";  // templates/cms/cms_sign.html
    }

    /** URL 생성 API (로그인 필요) */
    @ResponseBody
    @PostMapping("/api/cms/member/generate-sign-url")
    public AjaxResult generateSignUrl(
            @RequestParam("member_id") Long memberId,
            Authentication auth) {

        AjaxResult result = new AjaxResult();
        try {
            User user = (User) auth.getPrincipal();
            String url = cmsSignService.generateSignUrl(memberId, user.getSpjangcd());
            result.data = url;
        } catch (Exception e) {
            result.success = false;
            result.message = e.getMessage();
        }
        return result;
    }

    /** 납부자 데이터 조회 (토큰 검증) */
    @ResponseBody
    @GetMapping("/api/cms/sign/{token}")
    public AjaxResult getSignData(@PathVariable String token) {
        AjaxResult result = new AjaxResult();
        try {
            Map<String, Object> data = cmsSignService.getSignData(token);
            result.data = data;
        } catch (IllegalStateException e) {
            result.success = false;
            result.message = e.getMessage();
        } catch (Exception e) {
            result.success = false;
            result.message = "유효하지 않은 링크입니다.";
        }
        return result;
    }

    /** 서명 제출 */
    @ResponseBody
    @PostMapping("/api/cms/sign/submit")
    public AjaxResult submitSign(@RequestBody Map<String, String> payload) {
        AjaxResult result = new AjaxResult();
        try {
            cmsSignService.submitSign(payload);
        } catch (IllegalStateException e) {
            result.success = false;
            result.message = e.getMessage();
        } catch (Exception e) {
            result.success = false;
            result.message = "제출에 실패했습니다: " + e.getMessage();
        }
        return result;
    }

    /** 빈 양식 다운로드 (NCP에서 스트리밍) */
    @GetMapping("/api/cms/sign/form-download")
    public void downloadForm(javax.servlet.http.HttpServletResponse response) throws Exception {
        try {
            cmsSignService.streamFormFile(response);
        } catch (Exception e) {
            response.sendError(javax.servlet.http.HttpServletResponse.SC_NOT_FOUND, "양식 파일을 찾을 수 없습니다.");
        }
    }

    /** 파일 첨부 제출 (multipart) */
    @ResponseBody
    @PostMapping("/api/cms/sign/submit-file")
    public AjaxResult submitFile(
            @RequestParam("token") String token,
            @RequestParam("file") org.springframework.web.multipart.MultipartFile file,
            @RequestParam("account_holder") String accountHolder,
            @RequestParam("id_number") String idNumber,
            @RequestParam("bank_name") String bankName,
            @RequestParam("bank_account") String bankAccount,
            @RequestParam("phone") String phone) {
        AjaxResult result = new AjaxResult();
        try {
            cmsSignService.submitFile(token, file, accountHolder, idNumber, bankName, bankAccount, phone);
        } catch (IllegalStateException e) {
            result.success = false;
            result.message = e.getMessage();
        } catch (Exception e) {
            result.success = false;
            result.message = "제출에 실패했습니다: " + e.getMessage();
        }
        return result;
    }
}