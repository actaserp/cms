package mes.app.cms;

import mes.app.cms.service.*;
import mes.app.common.TenantContext;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/cms/account-register")
public class CmsAccountRegisterController {

    @Autowired private CmsAccountRegisterService cmsAccountRegisterService;
    @Autowired private CmsEi13SendService cmsEi13SendService;
    @Autowired private CmsEb13SendService cmsEb13SendService;
    @Autowired private CmsEb14ReceiveService cmsEb14ReceiveService;

    /** 목록 조회 */
    @GetMapping("/list")
    public AjaxResult getList(
            @RequestParam(required = false) String memberName,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) Long memberId) {
        AjaxResult result = new AjaxResult();
        result.data = cmsAccountRegisterService.getList(memberName, status, memberId);
        return result;
    }

    /** EI13 송신 */
    @PostMapping("/send-ei13")
    public AjaxResult sendEi13(@RequestParam("ids") String idsStr) {
        List<Long> ids = parseIds(idsStr);
        if (ids.isEmpty()) {
            AjaxResult r = new AjaxResult();
            r.success = false;
            r.message = "대상을 선택하세요.";
            return r;
        }
        AjaxResult result = new AjaxResult();
        result.data = cmsEi13SendService.send(ids);
        return result;
    }

    /** EB13 송신 */
    @PostMapping("/send-eb13")
    public AjaxResult sendEb13(@RequestParam("ids") String idsStr) {
        List<Long> ids = parseIds(idsStr);
        if (ids.isEmpty()) {
            AjaxResult r = new AjaxResult();
            r.success = false;
            r.message = "대상을 선택하세요.";
            return r;
        }
        AjaxResult result = new AjaxResult();
        result.data = cmsEb13SendService.send(ids);
        return result;
    }

    /** EB14 수신 (수동) */
    @PostMapping("/receive-eb14")
    public AjaxResult receiveEb14() {
        try {
            String spjangcd = TenantContext.get();
            cmsEb14ReceiveService.receive(spjangcd);
            AjaxResult result = new AjaxResult();
            result.message = "EB14 수신 완료";
            return result;
        } catch (Exception e) {
            AjaxResult result = new AjaxResult();
            result.success = false;
            result.message = e.getMessage();
            return result;
        }
    }

    /** 신규 등록 (납부자 저장 시 자동 생성용) */
    @PostMapping("/save")
    public AjaxResult save(
            @RequestParam("member_id") Long memberId,
            @RequestParam(value = "agree_type", required = false) String agreeType,
            @RequestParam(value = "agree_ext",  required = false) String agreeExt,
            @RequestParam(value = "agree_file_path", required = false) String agreeFilePath,
            Authentication auth) {
        User user = (User) auth.getPrincipal();
        AjaxResult result = new AjaxResult();
        result.data = cmsAccountRegisterService.save(memberId, agreeType, agreeExt, agreeFilePath, user.getUsername());
        return result;
    }

    private List<Long> parseIds(String idsStr) {
        return Arrays.stream(idsStr.split(","))
                .map(String::trim).filter(s -> !s.isEmpty())
                .map(Long::parseLong).collect(Collectors.toList());
    }

    /** 계좌등록 신청 — EI13 → EB13 자동 순서 처리 */
    @PostMapping("/register")
    public AjaxResult register(@RequestParam("ids") String idsStr) {
        List<Long> ids = parseIds(idsStr);
        if (ids.isEmpty()) {
            AjaxResult r = new AjaxResult(); r.success = false;
            r.message = "신청할 항목을 선택하세요."; return r;
        }
        AjaxResult result = new AjaxResult();
        result.data = cmsAccountRegisterService.register(ids);
        return result;
    }

    /** 재신청 — REJECTED 건 새 PENDING 생성 */
    @PostMapping("/re-register")
    public AjaxResult reRegister(@RequestParam("ids") String idsStr) {
        List<Long> ids = parseIds(idsStr);
        if (ids.isEmpty()) {
            AjaxResult r = new AjaxResult(); r.success = false;
            r.message = "재신청할 항목을 선택하세요."; return r;
        }

        Map<String, Object> res = cmsAccountRegisterService.reRegister(ids);

        int sentCnt  = res.get("sent")   != null ? ((Number) res.get("sent")).intValue()   : 0;
        int failedCnt = res.get("failed") != null ? ((Number) res.get("failed")).intValue() : 0;

        AjaxResult result = new AjaxResult();
        result.data = res;
        if (sentCnt == 0 && failedCnt > 0) {
            result.success = false;
            result.message = "재신청 실패: " + failedCnt + "건";
        } else if (failedCnt > 0) {
            result.message = "일부 실패: 성공 " + sentCnt + "건, 실패 " + failedCnt + "건";
        }
        return result;
    }

    /** 동의서 파일 첨부/변경 */
    @PostMapping("/update-file")
    public AjaxResult updateFile(
            @RequestParam("id")        Long registerId,
            @RequestParam("member_id") Long memberId) {
        AjaxResult result = new AjaxResult();
        cmsAccountRegisterService.updateAgreeFile(registerId, memberId);
        return result;
    }

    @PostMapping("/clear-file")
    public AjaxResult clearFile(@RequestParam("id") Long registerId) {
        AjaxResult result = new AjaxResult();
        cmsAccountRegisterService.clearAgreeFile(registerId);
        return result;
    }
}