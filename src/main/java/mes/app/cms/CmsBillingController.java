package mes.app.cms;

import lombok.extern.slf4j.Slf4j;
import mes.app.Scheduler.SchedulerService.CmsEb21SendService;
import mes.app.Scheduler.SchedulerService.CmsEb22ReceiveService;
import mes.app.Scheduler.SchedulerService.CmsEc21SendService;
import mes.app.Scheduler.SchedulerService.CmsEc22ReceiveService;
import mes.app.cms.service.CmsBillingService;
import mes.app.cms.service.CmsHolidayService;
import mes.app.common.TenantContext;
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

@Slf4j
@RestController
@RequestMapping("/api/cms/billing")
public class CmsBillingController {

    @Autowired
    private CmsBillingService cmsBillingService;

    @Autowired
    private CmsEb21SendService cmsEb21SendService;

    @Autowired
    private CmsEc21SendService cmsEc21SendService;

    @Autowired
    private CmsHolidayService cmsHolidayService;

    @Autowired
    private CmsEb22ReceiveService cmsEb22ReceiveService;

    @Autowired
    private CmsEc22ReceiveService cmsEc22ReceiveService;

    /** 목록 조회 */
    @GetMapping("/list")
    public AjaxResult getList(
            @RequestParam(value = "billing_ym"                       ) String billingYm,
            @RequestParam(value = "send_date_from", required = false) String sendDateFrom,
            @RequestParam(value = "send_date_to",   required = false) String sendDateTo,
            @RequestParam(value = "member_name",    required = false) String memberName,
            @RequestParam(value = "status",         required = false) String status,
            @RequestParam(value = "deduct_type",    required = false) String deductType,
            @RequestParam(defaultValue = "0")  int page,
            @RequestParam(defaultValue = "10") int size,
            HttpServletRequest request) {

        AjaxResult result = new AjaxResult();
        result.data = cmsBillingService.getBillingList(billingYm, sendDateFrom, sendDateTo, memberName, status, deductType, page, size);
        return result;
    }

    /**
     * 출금일 기준 신청 마감일(D-1 영업일) 계산
     * GET /api/cms/billing/send-date?deduct_date=20260525
     */
    @GetMapping("/send-date")
    public AjaxResult getSendDate(@RequestParam("deduct_date") String deductDate) {
        AjaxResult result = new AjaxResult();
        try {
            java.time.LocalDate d = java.time.LocalDate.parse(deductDate,
                    java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd")).minusDays(1);
            String sendDate = cmsHolidayService.getPrevBusinessDay(
                    d.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd")));
            result.data = java.util.Map.of("send_date", sendDate);
        } catch (Exception e) {
            result.success = false;
            result.message = e.getMessage();
        }
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
    public AjaxResult saveBilling(
            @RequestParam(value = "id",             required = false) Long   id,
            @RequestParam(value = "billing_ym",     required = false) String billingYm,
            @RequestParam(value = "member_id",      required = false) String memberId,
            @RequestParam(value = "member_name",    required = false) String memberName,
            @RequestParam(value = "bank_code",      required = false) String bankCode,
            @RequestParam(value = "bank_account",   required = false) String bankAccount,
            @RequestParam(value = "account_holder", required = false) String accountHolder,
            @RequestParam(value = "billing_amount", required = false) Long   billingAmount,
            @RequestParam(value = "deduct_day",     required = false) String deductDay,
            @RequestParam(value = "deduct_date",    required = false) String deductDate,
            @RequestParam(value = "status",         required = false) String status,
            @RequestParam(value = "memo",           required = false) String memo,
            @RequestParam(value = "deduct_type",    required = false) String deductType,
            Authentication auth) {

        User user = (User) auth.getPrincipal();


        AjaxResult result = new AjaxResult();

        try {
            String userId = user.getUsername();
            Long billingId = cmsBillingService.saveBilling(
                    id, billingYm, memberId, memberName, bankCode, bankAccount,
                    accountHolder, billingAmount, deductDay, deductDate,
                    status, memo, deductType, userId);

            result.data = billingId;
            result.message = "청구가 저장되었습니다.";

        } catch (IllegalStateException e) {
            // ⭐ 중지 기간 경고 메시지 처리
            if (e.getMessage() != null && e.getMessage().contains("중지 기간")) {
                result.success = false;
                result.message = e.getMessage();  // "이 납부자는 현재 중지 기간입니다. (중지기간: ...) 계속 청구하시겠습니까?"
                log.warn("[CmsBillingController] 중지 기간 경고 - message: {}", e.getMessage());
            } else {
                result.success = false;
                result.message = e.getMessage();
            }
        } catch (Exception e) {
            result.success = false;
            result.message = "저장에 실패했습니다: " + e.getMessage();
            log.error("[CmsBillingController] 저장 실패", e);
        }

        return result;
    }

    @PostMapping("/save-force")
    public AjaxResult saveBillingForce(
            @RequestParam(value = "id",             required = false) Long   id,
            @RequestParam(value = "billing_ym",     required = false) String billingYm,
            @RequestParam(value = "member_id",      required = false) String memberId,
            @RequestParam(value = "member_name",    required = false) String memberName,
            @RequestParam(value = "bank_code",      required = false) String bankCode,
            @RequestParam(value = "bank_account",   required = false) String bankAccount,
            @RequestParam(value = "account_holder", required = false) String accountHolder,
            @RequestParam(value = "billing_amount", required = false) Long   billingAmount,
            @RequestParam(value = "deduct_day",     required = false) String deductDay,
            @RequestParam(value = "deduct_date",    required = false) String deductDate,
            @RequestParam(value = "status",         required = false) String status,
            @RequestParam(value = "memo",           required = false) String memo,
            @RequestParam(value = "deduct_type",    required = false) String deductType,
            Authentication auth) {
        User user = (User) auth.getPrincipal();
        AjaxResult result = new AjaxResult();

        try {
            String userId = user.getUsername();
            Long billingId = cmsBillingService.saveBillingForce(
                    id, billingYm, memberId, memberName, bankCode, bankAccount,
                    accountHolder, billingAmount, deductDay, deductDate,
                    status, memo, deductType, userId);

            result.data = billingId;
            result.message = "청구가 등록되었습니다.";

        } catch (Exception e) {
            result.success = false;
            result.message = "저장에 실패했습니다: " + e.getMessage();
            log.error("[CmsBillingController] 강제 저장 실패", e);
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
            @RequestParam(defaultValue = "0")  int page,
            @RequestParam(defaultValue = "10") int size,
            HttpServletRequest request) {

        AjaxResult result = new AjaxResult();
        result.data = cmsBillingService.getBillingResultList(billingYm, resultDate, status, memberName, deductType, page, size);
        return result;
    }

    /** 불능 건 재청구 */
    @PostMapping("/recharge")
    public AjaxResult recharge(
            @RequestParam("ids") String idsStr,
            @RequestParam("deduct_dates") String datesStr,
            Authentication auth) {

        List<Long> ids = Arrays.stream(idsStr.split(","))
                .map(String::trim).filter(s -> !s.isEmpty())
                .map(Long::parseLong).collect(Collectors.toList());

        List<String> dates = Arrays.stream(datesStr.split(","))
                .map(String::trim).collect(Collectors.toList());

        if (ids.isEmpty()) {
            AjaxResult result = new AjaxResult();
            result.success = false;
            result.message = "재청구할 항목을 선택하세요.";
            return result;
        }

        User user = (User) auth.getPrincipal();
        Map<String, Object> res = cmsBillingService.rechargeBilling(ids, dates, user.getUsername());

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

        int sentCnt  = res.get("sent")   != null ? ((Number) res.get("sent")).intValue()   : 0;
        int failedCnt = res.get("failed") != null ? ((Number) res.get("failed")).intValue() : 0;

        AjaxResult result = new AjaxResult();
        result.data = res;
        if (sentCnt == 0 && failedCnt > 0) {
            result.success = false;
            result.message = "전송 실패: " + failedCnt + "건";
        }
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
            @RequestParam(value = "recharge_filter", required = false, defaultValue = "false") boolean rechargeFilter,
            @RequestParam(defaultValue = "0")  int page,
            @RequestParam(defaultValue = "10") int size,
            HttpServletRequest request) {

        AjaxResult result = new AjaxResult();
        if (rechargeFilter) {
            // 재청구용: 페이징 없이 전체 목록 (기존 동작 유지)
            result.data = cmsBillingService.getBillingHistoryForRecharge(startDate, endDate, billingType);
        } else {
            result.data = cmsBillingService.getBillingHistoryList(
                    startDate, endDate, billingType, status, memberName, page, size);
        }
        return result;
    }

    /** 즉시전송 — 체크된 PENDING 건 선택 후 SFTP 즉시 전송 (테스트/수동용) */
    @PostMapping("/send-now")
    public AjaxResult sendNow(
            @RequestParam("deduct_date") String deductDate,
            @RequestParam("deduct_type") String deductType) {

        String spjangcd = TenantContext.get();
        Map<String, Object> res = "EC".equals(deductType)
                ? cmsEc21SendService.runForSpjang(spjangcd, deductDate, "MANUAL")
                : cmsEb21SendService.runForSpjang(spjangcd, deductDate, "MANUAL");

        AjaxResult result = new AjaxResult();
        Map<String, Object> data = new java.util.HashMap<>();
        if (res.containsKey("error")) {
            data.put("sent", 0);
            data.put("failed", 1);
        } else {
            data.put("sent", 1);
            data.put("failed", 0);
        }
        result.data = data;
        return result;
    }

    @GetMapping("/sendable-dates")
    public AjaxResult getSendableDates(
            @RequestParam("billing_ym") String billingYm,
            @RequestParam(value = "deduct_type", required = false) String deductType) {
        AjaxResult result = new AjaxResult();
        result.data = cmsBillingService.getSendableDates(billingYm, deductType);
        return result;
    }

    @GetMapping("/available-files")
    public AjaxResult getAvailableFiles(
            @RequestParam(value = "deduct_type", required = false, defaultValue = "EB") String deductType) {
        AjaxResult result = new AjaxResult();
        try {
            String spjangcd = TenantContext.get();
            List<Map<String, Object>> files = "EC".equals(deductType)
                    ? cmsEc22ReceiveService.getAvailableEc22Files(spjangcd)
                    : cmsEb22ReceiveService.getAvailableEb22Files(spjangcd);
            result.data = files;
        } catch (Exception e) {
            result.success = false;
            result.message = "파일 목록 조회 실패: " + e.getMessage();
        }
        return result;
    }

    /** EB22 선택 파일 수동 수신 */
    @PostMapping("/receive-file")
    public AjaxResult receiveFile(
            @RequestParam("fileName") String fileName,
            @RequestParam(value = "deduct_type", required = false, defaultValue = "EB") String deductType) {
        AjaxResult result = new AjaxResult();
        try {
            String spjangcd = TenantContext.get();

            Map<String, Object> processResult = "EC".equals(deductType)
                    ? cmsEc22ReceiveService.processSelectedEc22File(spjangcd, fileName)
                    : cmsEb22ReceiveService.processSelectedEb22File(spjangcd, fileName);

            if ((Boolean) processResult.get("success")) {
                result.data = processResult;
            } else {
                result.success = false;
                result.message = (String) processResult.get("message");
            }
        } catch (Exception e) {
            result.success = false;
            result.message = "파일 처리 실패: " + e.getMessage();
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

    @PostMapping("/import-from-erp")
    public AjaxResult importFromErp(
            @RequestParam("billing_ym") String billingYm,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();
        try {
            result.data = cmsBillingService.importFromErp(billingYm, user.getUsername());
        } catch (Exception e) {
            result.success = false;
            result.message = e.getMessage();
        }
        return result;
    }

    /** 약정일별 수동 청구 가능 건수 조회 */
    @GetMapping("/deduct-day-summary")
    public AjaxResult getDeductDaySummary(
            @RequestParam("billing_ym") String billingYm,
            @RequestParam(value = "deduct_type", required = false) String deductType) {
        AjaxResult result = new AjaxResult();
        result.data = cmsBillingService.getDeductDaySummary(billingYm, deductType);
        return result;
    }

    /** 수동 청구 생성 - 선택한 약정일 기준, send_date = 오늘 */
    @PostMapping("/generate-manual")
    public AjaxResult generateManual(
            @RequestParam("billing_ym")               String billingYm,
            @RequestParam("deduct_days")              String deductDaysStr,
            @RequestParam(value = "deduct_type", required = false) String deductType,
            Authentication auth) {

        User user = (User) auth.getPrincipal();
        AjaxResult result = new AjaxResult();
        try {
            List<String> deductDays = Arrays.stream(deductDaysStr.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList());

            result.data = cmsBillingService.generateBillingManual(
                    billingYm, deductDays, deductType, user.getUsername());
        } catch (Exception e) {
            result.success = false;
            result.message = "청구 생성 실패: " + e.getMessage();
            log.error("[CmsBillingController] 수동 청구 생성 실패", e);
        }
        return result;
    }

    @PostMapping("/change-deduct-date")
    public AjaxResult changeDeductDate(
            @RequestParam String ids,
            @RequestParam String deduct_date) {
        int count = cmsBillingService.changeDeductDate(ids, deduct_date);
        AjaxResult result = new AjaxResult();
        result.data = count;
        return result;
    }
}