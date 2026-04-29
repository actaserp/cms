package mes.app.cms;

import mes.app.Scheduler.SchedulerService.CmsEb21SendService;
import mes.app.Scheduler.SchedulerService.CmsEc21SendService;
import mes.app.cms.service.CmsBillingService;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/cms/billing")
public class CmsBillingController {

    @Autowired
    private CmsBillingService cmsBillingService;

    @Autowired
    private CmsEb21SendService cmsEb21SendService;

    @Autowired
    private CmsEc21SendService cmsEc21SendService;

    /** 목록 조회 */
    @GetMapping("/list")
    public AjaxResult getList(
            @RequestParam(value = "billing_ym"                   ) String billingYm,
            @RequestParam(value = "deduct_date", required = false) String deductDate,
            @RequestParam(value = "member_name", required = false) String memberName,
            @RequestParam(value = "status",      required = false) String status,
            @RequestParam(value = "deduct_type", required = false) String deductType,
            HttpServletRequest request) {

        List<Map<String, Object>> items = cmsBillingService.getBillingList(billingYm, deductDate, memberName, status, deductType);
        AjaxResult result = new AjaxResult();
        result.data = items;
        return result;
    }

    /** 단건 조회 */
    @GetMapping("/{id}")
    public AjaxResult getOne(@PathVariable Long id) {
        Map<String, Object> item = cmsBillingService.getBilling(id);
        AjaxResult result = new AjaxResult();
        result.data = item;
        return result;
    }

    /** 등록/수정 */
    @PostMapping("/save")
    public AjaxResult save(
            @RequestParam(value = "id",              required = false) Long   id,
            @RequestParam(value = "billing_ym"                       ) String billingYm,
            @RequestParam(value = "member_id",       required = false) String memberId,
            @RequestParam(value = "member_name",     required = false) String memberName,
            @RequestParam(value = "bank_code",       required = false) String bankCode,
            @RequestParam(value = "bank_account",    required = false) String bankAccount,
            @RequestParam(value = "account_holder",  required = false) String accountHolder,
            @RequestParam(value = "billing_amount",  required = false) Long   billingAmount,
            @RequestParam(value = "deduct_day",      required = false) String deductDay,
            @RequestParam(value = "deduct_date",     required = false) String deductDate,
            @RequestParam(value = "status",          required = false) String status,
            @RequestParam(value = "memo",            required = false) String memo,
            @RequestParam(value = "deduct_type",     required = false) String deductType,
            Authentication auth) {

        User user = (User) auth.getPrincipal();
        Long savedId = cmsBillingService.saveBilling(
                id, billingYm, memberId, memberName, bankCode, bankAccount, accountHolder,
                billingAmount, deductDay, deductDate, status, memo, deductType, user.getUsername());

        AjaxResult result = new AjaxResult();
        if (savedId == null) {
            result.success = false;
            result.message = "저장에 실패했습니다.";
        } else {
            result.data = savedId;
        }
        return result;
    }

    /** 삭제 (PENDING 건만 허용) */
    @DeleteMapping("/{id}")
    public AjaxResult delete(@PathVariable Long id) {
        AjaxResult result = new AjaxResult();
        boolean ok = cmsBillingService.deleteBilling(id);
        if (!ok) {
            result.success = false;
            result.message = "삭제 실패 — PENDING 상태인 건만 삭제할 수 있습니다.";
        }
        return result;
    }

    /** 수납결과 조회 */
    @GetMapping("/result/list")
    public AjaxResult getResultList(
            @RequestParam(value = "billing_ym"                   ) String billingYm,
            @RequestParam(value = "result_date", required = false) String resultDate,
            @RequestParam(value = "status",      required = false) String status,
            @RequestParam(value = "member_name", required = false) String memberName,
            @RequestParam(value = "deduct_type", required = false) String deductType,
            HttpServletRequest request) {

        List<Map<String, Object>> items = cmsBillingService.getBillingResultList(billingYm, resultDate, status, memberName, deductType);
        AjaxResult result = new AjaxResult();
        result.data = items;
        return result;
    }

    /** 불능 건 재청구 */
    @PostMapping("/recharge")
    public AjaxResult recharge(
            @RequestParam("ids") String idsStr,
            Authentication auth) {

        List<Long> ids = Arrays.stream(idsStr.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(Long::parseLong)
                .collect(Collectors.toList());

        if (ids.isEmpty()) {
            AjaxResult result = new AjaxResult();
            result.success = false;
            result.message = "재청구할 항목을 선택하세요.";
            return result;
        }

        User user = (User) auth.getPrincipal();
        Map<String, Object> res = cmsBillingService.rechargeBilling(ids, user.getUsername());

        AjaxResult result = new AjaxResult();
        result.data = res;
        return result;
    }

    /** 청구 자동생성 */
    @PostMapping("/generate")
    public AjaxResult generate(
            @RequestParam("billing_ym")                        String billingYm,
            @RequestParam(value = "deduct_type", required = false) String deductType,
            Authentication auth) {

        User user = (User) auth.getPrincipal();
        Map<String, Object> res = cmsBillingService.generateBilling(billingYm, deductType, user.getUsername());

        AjaxResult result = new AjaxResult();
        result.data = res;
        return result;
    }

    /** 청구 취소 (선택된 PENDING 건 → CANCEL) */
    @PostMapping("/cancel")
    public AjaxResult cancel(
            @RequestParam("ids") String idsStr,
            Authentication auth) {

        List<Long> ids = Arrays.stream(idsStr.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(Long::parseLong)
                .collect(Collectors.toList());

        if (ids.isEmpty()) {
            AjaxResult result = new AjaxResult();
            result.success = false;
            result.message = "취소할 항목을 선택하세요.";
            return result;
        }

        User user = (User) auth.getPrincipal();
        int count = cmsBillingService.cancelBilling(ids, user.getUsername());

        AjaxResult result = new AjaxResult();
        result.data = count;
        if (count == 0) {
            result.success = false;
            result.message = "취소 가능한 건이 없습니다. (PENDING 상태만 취소 가능)";
        }
        return result;
    }

    /** 수동 재전송 — PENDING 건 선택 후 SFTP 재전송 */
    @PostMapping("/resend")
    public AjaxResult resend(
            @RequestParam("ids")         String idsStr,
            @RequestParam("deduct_type") String deductType) {

        List<Long> ids = Arrays.stream(idsStr.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(Long::parseLong)
                .collect(Collectors.toList());

        if (ids.isEmpty()) {
            AjaxResult result = new AjaxResult();
            result.success = false;
            result.message = "재전송할 항목을 선택하세요.";
            return result;
        }

        Map<String, Object> res = "EC".equals(deductType)
                ? cmsEc21SendService.resendBilling(ids)
                : cmsEb21SendService.resendBilling(ids);

        AjaxResult result = new AjaxResult();
        result.data = res;
        return result;
    }

    /** 수납내역 조회 (기간, EB+EC 통합) */
    @GetMapping("/history/list")
    public AjaxResult getHistoryList(
            @RequestParam(value = "start_date"                   ) String startDate,
            @RequestParam(value = "end_date"                     ) String endDate,
            @RequestParam(value = "billing_type", required = false) String billingType,
            @RequestParam(value = "status",       required = false) String status,
            @RequestParam(value = "member_name",  required = false) String memberName,
            HttpServletRequest request) {

        List<Map<String, Object>> items = cmsBillingService.getBillingHistoryList(startDate, endDate, billingType, status, memberName);
        AjaxResult result = new AjaxResult();
        result.data = items;
        return result;
    }
}
