package mes.app.cms;

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

    /** 목록 조회 */
    @GetMapping("/list")
    public AjaxResult getList(
            @RequestParam(value = "billing_ym"              ) String billingYm,
            @RequestParam(value = "member_name", required = false) String memberName,
            @RequestParam(value = "status",      required = false) String status,
            HttpServletRequest request) {

        List<Map<String, Object>> items = cmsBillingService.getBillingList(billingYm, memberName, status);
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
            Authentication auth) {

        User user = (User) auth.getPrincipal();
        Long savedId = cmsBillingService.saveBilling(
                id, billingYm, memberId, memberName, bankCode, bankAccount, accountHolder,
                billingAmount, deductDay, deductDate, status, memo, user.getUsername());

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

    /** 청구 자동생성 */
    @PostMapping("/generate")
    public AjaxResult generate(
            @RequestParam("billing_ym") String billingYm,
            Authentication auth) {

        User user = (User) auth.getPrincipal();
        Map<String, Object> res = cmsBillingService.generateBilling(billingYm, user.getUsername());

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
}
