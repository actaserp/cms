package mes.app.cms;

import mes.app.Scheduler.SchedulerService.CmsBillingAutoGenerateService;
import mes.app.cms.service.CmsMemberService;
import mes.app.common.TenantContext;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/cms/member")
public class CmsMemberController {

    @Autowired
    private CmsMemberService cmsMemberService;

    @Autowired
    private CmsBillingAutoGenerateService cmsBillingAutoGenerateService;

    /** 목록 조회 */
    @GetMapping("/list")
    public AjaxResult getList(
            @RequestParam(value = "member_name", required = false) String memberName,
            @RequestParam(value = "member_no",   required = false) String memberNo,
            @RequestParam(value = "keyword",     required = false) String keyword,
            @RequestParam(value = "status",      required = false) String status,
            HttpServletRequest request) {

        // keyword가 있으면 이름 OR 번호 통합검색(계좌조회 모달용), 없으면 기존 방식 그대로
        List<Map<String, Object>> items = cmsMemberService.getMemberList(memberName, memberNo, keyword, status);
        AjaxResult result = new AjaxResult();
        result.data = items;
        return result;
    }

    /** 단건 조회 */
    @GetMapping("/{id}")
    public AjaxResult getOne(@PathVariable Long id) {
        Map<String, Object> item = cmsMemberService.getMember(id);
        AjaxResult result = new AjaxResult();
        result.data = item;
        return result;
    }

    /** 등록/수정 */
    @PostMapping("/save")
    public AjaxResult save(
            @RequestParam(value = "id",                required = false) Long    id,
            @RequestParam(value = "member_type",       required = false) String  memberType,
            @RequestParam(value = "member_name"                        ) String  memberName,
            @RequestParam(value = "member_no",         required = false) String  memberNo,
            @RequestParam(value = "id_number",         required = false) String  idNumber,
            @RequestParam(value = "resident_no",       required = false) String  residentNo,
            @RequestParam(value = "phone",             required = false) String  phone,
            @RequestParam(value = "email",             required = false) String  email,
            @RequestParam(value = "zipcd",             required = false) String  zipcd,
            @RequestParam(value = "adresa",            required = false) String  adresa,
            @RequestParam(value = "adresb",            required = false) String  adresb,
            @RequestParam(value = "bank_code",         required = false) String  bankCode,
            @RequestParam(value = "bank_account",      required = false) String  bankAccount,
            @RequestParam(value = "account_holder",    required = false) String  accountHolder,
            @RequestParam(value = "deduct_day",        required = false) String  deductDay,
            @RequestParam(value = "deduct_amount",     required = false) Long    deductAmount,
            @RequestParam(value = "cycle_type",        required = false) String  cycleType,
            @RequestParam(value = "cycle_months",      required = false) String  cycleMonths,
            @RequestParam(value = "deduct_month_type", required = false) String  deductMonthType,
            @RequestParam(value = "start_date",        required = false) String  startDate,
            @RequestParam(value = "end_date",          required = false) String  endDate,
            @RequestParam(value = "pause_start_date",  required = false) String  pauseStartDate,  // 추가됨
            @RequestParam(value = "pause_end_date",    required = false) String  pauseEndDate,    // 추가됨
            @RequestParam(value = "pause_reason",      required = false) String  pauseReason,     // 추가됨
            @RequestParam(value = "agree_yn",          required = false) String  agreeYn,
            @RequestParam(value = "agree_method",      required = false) String  agreeMethod,
            @RequestParam(value = "status",            required = false) String  status,
            @RequestParam(value = "memo",              required = false) String  memo,
            Authentication auth) {

        User user = (User) auth.getPrincipal();

        Long savedId = cmsMemberService.saveMember(
                id, memberType, memberName, memberNo, idNumber, residentNo,
                phone, email, zipcd, adresa, adresb,
                bankCode, bankAccount, accountHolder,
                deductDay, deductAmount, cycleType, cycleMonths, deductMonthType, startDate, endDate,
                pauseStartDate, pauseEndDate, pauseReason,  // 추가됨
                agreeYn, agreeMethod, status, memo,
                user.getUsername());

        AjaxResult result = new AjaxResult();
        if (savedId == null) {
            result.success = false;
            result.message = "저장에 실패했습니다.";
        } else {
            result.data = savedId;
            // 신규 등록(id == null)이고 ACTIVE 상태면 이번달 청구 즉시 생성
//            if (id == null && "ACTIVE".equals(status)) {
//                try {
//                    cmsBillingAutoGenerateService.generateForNewMember(
//                            TenantContext.get(), savedId, user.getUsername());
//                } catch (Exception e) {
//                    // 청구 생성 실패는 납부자 저장 자체를 롤백하지 않음 — 로그만
//                    result.message = "납부자 등록 완료. 이번달 청구 자동생성 중 오류가 발생했습니다.";
//                }
//            }
        }
        return result;
    }

    /** 삭제 (soft delete: status = INACTIVE) */
    @DeleteMapping("/{id}")
    public AjaxResult delete(@PathVariable Long id) {
        AjaxResult result = new AjaxResult();
        boolean ok = cmsMemberService.deleteMember(id);
        if (!ok) {
            result.success = false;
            result.message = "삭제에 실패했습니다.";
        }
        return result;
    }

    @PostMapping("/excel-upload")
    public AjaxResult excelUpload(@RequestParam("file") MultipartFile file, Authentication auth) {
        User user = (User) auth.getPrincipal();
        AjaxResult result = new AjaxResult();
        result.data = cmsMemberService.excelUpload(file, user.getUsername());
        return result;
    }

    @GetMapping("/excel-template")
    public void excelTemplate(HttpServletResponse response) throws Exception {
        cmsMemberService.downloadTemplate(response);
    }

    /** ERP 동기화 */
    @PostMapping("/erp-sync")
    public AjaxResult erpSync(Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();
        try {
            result.data = cmsMemberService.syncFromErp(user.getSpjangcd(), user.getUsername());
        } catch (Exception e) {
            result.success = false;
            result.message = e.getMessage();
        }
        return result;
    }

    /** ERP 동기화 미리보기 (읽기 전용) */
    @GetMapping("/erp-sync-preview")
    public AjaxResult erpSyncPreview(Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();
        try {
            result.data = cmsMemberService.previewSync(user.getSpjangcd());
        } catch (Exception e) {
            result.success = false;
            result.message = e.getMessage();
        }
        return result;
    }

    /** ERP 동기화 선택 반영 (회원 단위 통짜 — 구버전) */
    @PostMapping("/erp-sync-apply")
    public AjaxResult erpSyncApply(
            @RequestBody(required = false) List<String> selectedCltcds,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();
        try {
            result.data = cmsMemberService.applySync(user.getSpjangcd(), selectedCltcds, user.getUsername());
        } catch (Exception e) {
            result.success = false;
            result.message = e.getMessage();
        }
        return result;
    }

    /** ERP 동기화 필드별 선택 반영 + 확정 (신버전)
     *  body: [{ "cltcd":"...", "erpFields":["bank_account","deduct_amount"] }, ...] */
    @PostMapping("/erp-sync-apply-selective")
    public AjaxResult erpSyncApplySelective(
            @RequestBody(required = false) List<Map<String, Object>> decisions,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();
        try {
            result.data = cmsMemberService.applySyncSelective(user.getSpjangcd(), decisions, user.getUsername());
        } catch (Exception e) {
            result.success = false;
            result.message = e.getMessage();
        }
        return result;
    }

    /** ERP 동기화 오염 진단 (읽기 전용) */
    @GetMapping("/erp-sync-diagnose")
    public AjaxResult erpSyncDiagnose(Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();
        try {
            result.data = cmsMemberService.diagnoseSync(user.getSpjangcd());
        } catch (Exception e) {
            result.success = false;
            result.message = e.getMessage();
        }
        return result;
    }

    /** 해지 신청 — EB13 apply_type='3' 송신 후 PENDING_CANCEL */
    @PostMapping("/cancel")
    @ResponseBody
    public AjaxResult cancelMember(@RequestParam Long member_id, Authentication auth) {
        User user = (User) auth.getPrincipal();
        AjaxResult result = new AjaxResult();
        try {
            result.data = cmsMemberService.cancelMember(member_id, user.getUsername());
            result.success = true;
        } catch (Exception e) {
            result.success = false;
            result.message = e.getMessage();
        }
        return result;
    }

    /** 다건 해지 신청 — 체크된 회원 일괄 해지 (EB13 한 파일로 묶어 전송) */
    @PostMapping("/cancel-multi")
    @ResponseBody
    public AjaxResult cancelMembers(@RequestParam("member_ids") String memberIdsStr, Authentication auth) {
        User user = (User) auth.getPrincipal();
        AjaxResult result = new AjaxResult();

        List<Long> memberIds = java.util.Arrays.stream(memberIdsStr.split(","))
                .map(String::trim).filter(s -> !s.isEmpty())
                .map(Long::parseLong).collect(java.util.stream.Collectors.toList());

        if (memberIds.isEmpty()) {
            result.success = false;
            result.message = "해지할 대상을 선택하세요.";
            return result;
        }

        try {
            Map<String, Object> res = cmsMemberService.cancelMembers(memberIds, user.getUsername());
            int sent = res.get("sent") != null ? ((Number) res.get("sent")).intValue() : 0;
            result.data = res;
            if (sent == 0) {
                result.success = false;
                result.message = res.get("message") != null ? (String) res.get("message") : "해지 실패";
            } else {
                result.message = (String) res.get("message");
            }
        } catch (Exception e) {
            result.success = false;
            result.message = e.getMessage();
        }
        return result;
    }

    @PostMapping("/manual-agree")
    @ResponseBody
    public AjaxResult manualAgree(@RequestParam Long member_id, Authentication auth) {
        User user = (User) auth.getPrincipal();
        String userId = String.valueOf(user.getId());
        AjaxResult result = new AjaxResult();
        try {
            cmsMemberService.manualAgree(member_id, userId);
            result.success = true;
        } catch (Exception e) {
            result.success = false;
            result.message = e.getMessage();
        }
        return result;
    }

    /** 계좌변경 신청 — 구계좌 해지 + 신계좌 신규를 세트로 생성 */
    @PostMapping("/change-account")
    @ResponseBody
    public AjaxResult changeAccount(@RequestParam Long member_id,
                                    @RequestParam String bank_code,
                                    @RequestParam String bank_account,
                                    @RequestParam(required = false) String account_holder,
                                    Authentication auth) {
        User user = (User) auth.getPrincipal();
        String userId = String.valueOf(user.getId());
        AjaxResult result = new AjaxResult();
        try {
            Map<String, Object> res = cmsMemberService.changeAccount(
                    member_id, bank_code, bank_account, account_holder, userId);
            boolean ok = Boolean.TRUE.equals(res.get("success"));
            result.success = ok;
            result.message = (String) res.get("message");
        } catch (Exception e) {
            result.success = false;
            result.message = e.getMessage();
        }
        return result;
    }


    /** 저장된 계좌조회 결과 — 유료 재조회 전에 화면이 먼저 확인한다. */
    @GetMapping("/inquiry-cache")
    @ResponseBody
    public AjaxResult inquiryCache(@RequestParam("bank_code")  String bankCode,
                                   @RequestParam("account_no") String accountNo,
                                   Authentication auth) {
        User user = (User) auth.getPrincipal();
        AjaxResult result = new AjaxResult();
        try {
            result.data = cmsMemberService.getInquiryCache(user.getSpjangcd(), bankCode, accountNo);
        } catch (Exception e) {
            result.success = false;
            result.message = e.getMessage();
        }
        return result;
    }

    /** 실시간 계좌조회 — 금결원 예금주명 확인 (등록 여부와 무관, 예금주명 검증용) */
    @PostMapping("/account-inquiry")
    @ResponseBody
    public AjaxResult accountInquiry(@RequestParam("bank_code")          String bankCode,
                                     @RequestParam("account_no")         String accountNo,
                                     @RequestParam("identification_no")  String identificationNo,
                                     @RequestParam(value = "input_holder", required = false) String inputHolder,
                                     Authentication auth) {
        User user = (User) auth.getPrincipal();
        AjaxResult result = new AjaxResult();
        try {
            Map<String, Object> res = cmsMemberService.inquiryAccount(
                    user.getSpjangcd(), bankCode, accountNo, identificationNo, inputHolder);
            result.success = Boolean.TRUE.equals(res.get("success"));
            result.data = res;
            if (!result.success) {
                result.message = res.get("responseMessage") != null
                        ? (String) res.get("responseMessage") : "계좌조회에 실패했습니다.";
            }
        } catch (Exception e) {
            result.success = false;
            result.message = "계좌조회 오류: " + e.getMessage();
        }
        return result;
    }

}