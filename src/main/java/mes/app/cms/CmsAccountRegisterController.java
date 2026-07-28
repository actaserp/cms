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
            @RequestParam(required = false) Long memberId,
            @RequestParam(defaultValue = "0")  int page,
            @RequestParam(defaultValue = "10") int size) {
        AjaxResult result = new AjaxResult();
        result.data = cmsAccountRegisterService.getList(memberName, status, memberId, page, size);
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
    public AjaxResult receiveEb14(@RequestParam(required = false) String target_date) {
        AjaxResult result = new AjaxResult();
        try {
            String spjangcd = TenantContext.get();
            cmsEb14ReceiveService.receive(spjangcd, target_date);
            result.message = "EB14 수신 완료";
        } catch (Exception e) {
            result.success = false;
            result.message = e.getMessage();
        }
        return result;
    }

    @GetMapping("/eb14-file-list")
    public AjaxResult getEb14FileList() {
        AjaxResult result = new AjaxResult();
        try {
            String spjangcd = TenantContext.get();
            result.data = cmsEb14ReceiveService.getFileList(spjangcd);
        } catch (Exception e) {
            result.success = false;
            result.message = e.getMessage();
        }
        return result;
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

    /** 계좌등록 신청 — 신규 + 재신청(실패/거절) 통합, EI13 → EB13 자동 순서 처리 */
    @PostMapping("/register")
    public AjaxResult register(@RequestParam("ids") String idsStr) {
        List<Long> ids = parseIds(idsStr);
        if (ids.isEmpty()) {
            AjaxResult r = new AjaxResult(); r.success = false;
            r.message = "신청할 항목을 선택하세요."; return r;
        }
        Map<String, Object> res = cmsAccountRegisterService.register(ids);

        int sentCnt  = res.get("sent")   != null ? ((Number)res.get("sent")).intValue()   : 0;
        int failedCnt = res.get("failed") != null ? ((Number)res.get("failed")).intValue() : 0;
        String message = res.get("message") != null ? (String)res.get("message") : null;

        AjaxResult result = new AjaxResult();
        result.data = res;

        if (sentCnt == 0 && failedCnt > 0) {
            result.success = false;
            result.message = message != null ? message : "신청 실패: " + failedCnt + "건";
        }
        return result;
    }

    /** 대기건 신청 취소 — 행 삭제 + cms_member 원복 */
    @PostMapping("/cancel")
    public AjaxResult cancelPending(@RequestParam("ids") String idsStr, Authentication auth) {
        User user = (User) auth.getPrincipal();
        List<Long> ids = parseIds(idsStr);
        AjaxResult result = new AjaxResult();
        if (ids.isEmpty()) {
            result.success = false;
            result.message = "취소할 항목을 선택하세요.";
            return result;
        }
        try {
            Map<String, Object> res = cmsAccountRegisterService.cancelPending(ids, user.getUsername());
            int deleted = res.get("deleted") != null ? ((Number) res.get("deleted")).intValue() : 0;
            result.data = res;
            result.message = (String) res.get("message");
            if (deleted == 0) result.success = false;
        } catch (Exception e) {
            result.success = false;
            result.message = "취소 실패: " + e.getMessage();
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

    @PostMapping("/create-from-erp")
    @ResponseBody
    public AjaxResult createFromErp(Authentication auth) {
        User user = (User) auth.getPrincipal();
        String userId = String.valueOf(user.getId());
        AjaxResult result = new AjaxResult();
        try {
            Map<String, Object> res = cmsAccountRegisterService.createFromErpMembers(userId);
            result.success = true;
            result.data = res;
        } catch (Exception e) {
            result.success = false;
            result.message = e.getMessage();
        }
        return result;
    }

    @PostMapping("/preview-eb11")
    public AjaxResult previewEb11(@RequestParam Integer bbsseq) {
        AjaxResult result = new AjaxResult();
        try { result.data = cmsAccountRegisterService.previewEb11(bbsseq); }
        catch (Exception e) { result.success = false; result.message = e.getMessage(); }
        return result;
    }

    @PostMapping("/apply-eb11")
    public AjaxResult applyEb11(@RequestParam Integer bbsseq,
                                @RequestParam("member_ids") String memberIdsStr,
                                Authentication auth) {
        User user = (User) auth.getPrincipal();
        AjaxResult result = new AjaxResult();
        try {
            result.data = cmsAccountRegisterService.applyEb11(bbsseq, parseIds(memberIdsStr), user.getUsername());
        } catch (Exception e) { result.success = false; result.message = e.getMessage(); }
        return result;
    }
}