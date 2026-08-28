package mes.app.cms;

import lombok.extern.slf4j.Slf4j;
import mes.app.Scheduler.SchedulerService.CmsEb21SendService;
import mes.app.Scheduler.SchedulerService.CmsEb22ReceiveService;
import mes.app.Scheduler.SchedulerService.CmsEc21SendService;
import mes.app.Scheduler.SchedulerService.CmsEc22ReceiveService;
import mes.app.cms.service.CmsBillingService;
import mes.app.cms.service.CmsErpResultSyncService;
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

    @Autowired
    private CmsErpResultSyncService cmsErpResultSyncService;

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
            @RequestParam(value = "billing_ym",       required = false) String billingYm,
            @RequestParam(value = "deduct_date_from", required = false) String deductDateFrom,
            @RequestParam(value = "deduct_date_to",   required = false) String deductDateTo,
            @RequestParam(value = "result_date", required = false) String resultDate,
            @RequestParam(value = "status",      required = false) String status,
            @RequestParam(value = "member_name", required = false) String memberName,
            @RequestParam(value = "deduct_type", required = false) String deductType,
            @RequestParam(defaultValue = "0")  int page,
            @RequestParam(defaultValue = "10") int size,
            HttpServletRequest request) {

        AjaxResult result = new AjaxResult();
        result.data = cmsBillingService.getBillingResultList(
                billingYm, deductDateFrom, deductDateTo, resultDate, status, memberName, deductType, page, size);
        return result;
    }

    /** 불능 건 재청구 */
    @PostMapping("/recharge")
    public AjaxResult recharge(
            @RequestParam("ids") String idsStr,
            @RequestParam("deduct_dates") String datesStr,
            @RequestParam(value = "deduct_types", required = false) String typesStr,
            Authentication auth) {

        List<Long> ids = Arrays.stream(idsStr.split(","))
                .map(String::trim).filter(s -> !s.isEmpty())
                .map(Long::parseLong).collect(Collectors.toList());

        List<String> dates = Arrays.stream(datesStr.split(","))
                .map(String::trim).collect(Collectors.toList());

        // 건별 EB/EC 구분 (비어있으면 서비스에서 원본 타입 상속)
        List<String> types = (typesStr == null || typesStr.isBlank())
                ? java.util.Collections.emptyList()
                : Arrays.stream(typesStr.split(",", -1))
                .map(String::trim).collect(Collectors.toList());

        if (ids.isEmpty()) {
            AjaxResult result = new AjaxResult();
            result.success = false;
            result.message = "재청구할 항목을 선택하세요.";
            return result;
        }

        User user = (User) auth.getPrincipal();
        Map<String, Object> res = cmsBillingService.rechargeBilling(ids, dates, types, user.getUsername());

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

    /** 결과 ERP 반영 — 선택한 출금일들의 SUCCESS 결과를 tb_bank_cmssave에 반영(없으면 INSERT).
     *  body: { "deduct_dates": ["20260701","20260702"] } */
    @PostMapping("/erp-resync")
    public AjaxResult erpResync(@RequestBody Map<String, Object> body) {
        AjaxResult result = new AjaxResult();
        try {
            String spjangcd = TenantContext.get();
            @SuppressWarnings("unchecked")
            List<String> deductDates = body.get("deduct_dates") instanceof List
                    ? (List<String>) body.get("deduct_dates") : java.util.Collections.emptyList();
            if (deductDates.isEmpty()) {
                result.success = false;
                result.message = "반영할 출금일을 선택하세요.";
                return result;
            }
            int inserted = 0, skipped = 0, failed = 0;
            java.util.Set<String> done = new java.util.LinkedHashSet<>(deductDates);
            for (String dd : done) {
                if (dd == null || dd.isBlank()) continue;
                Map<String, Object> r = cmsErpResultSyncService.resyncByDeductDate(spjangcd, dd.replace("-", ""));
                inserted += ((Number) r.getOrDefault("inserted", 0)).intValue();
                skipped  += ((Number) r.getOrDefault("skipped", 0)).intValue();
                failed   += ((Number) r.getOrDefault("failed", 0)).intValue();
            }
            Map<String, Object> data = new java.util.LinkedHashMap<>();
            data.put("inserted", inserted);
            data.put("skipped", skipped);
            data.put("failed", failed);
            result.data = data;
        } catch (Exception e) {
            result.success = false;
            result.message = "ERP 반영 실패: " + e.getMessage();
        }
        return result;
    }

    /**
     * 엑셀 대량 업로드 (upsert)
     * - 청구번호(billing_seq)가 있으면 수정, 없으면 신규 등록
     * - 수정은 PENDING 상태 건만 허용, 나머지는 스킵
     * - 신규는 납부자번호로 회원을 찾아 출금이체 동의(agree_yn=Y) 확인
     * 화면에서 파싱한 행 배열(rows)을 JSON body로 받아 한 트랜잭션으로 처리한다.
     */
    @PostMapping("/bulk-upload")
    public AjaxResult bulkUpload(@RequestBody Map<String, Object> body, Authentication auth) {
        User user = (User) auth.getPrincipal();
        AjaxResult result = new AjaxResult();
        try {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> rows = body.get("rows") instanceof List
                    ? (List<Map<String, Object>>) body.get("rows") : java.util.Collections.emptyList();
            if (rows.isEmpty()) {
                result.success = false;
                result.message = "업로드할 데이터가 없습니다.";
                return result;
            }
            String defaultBillingYm = body.get("billing_ym") != null ? body.get("billing_ym").toString() : null;
            String deductType = body.get("deduct_type") != null ? body.get("deduct_type").toString() : "EB";

            Map<String, Object> data = cmsBillingService.bulkUpsertBilling(
                    rows, defaultBillingYm, deductType, user.getUsername());

            boolean applied = Boolean.TRUE.equals(data.get("applied"));
            int inserted = ((Number) data.getOrDefault("inserted", 0)).intValue();
            int updated  = ((Number) data.getOrDefault("updated", 0)).intValue();
            int skipped  = ((Number) data.getOrDefault("skipped", 0)).intValue();
            int failed   = ((Number) data.getOrDefault("failed", 0)).intValue();

            result.data = data;
            if (applied) {
                result.message = String.format("반영 완료 — 신규 %d건, 수정 %d건, 스킵 %d건", inserted, updated, skipped);
            } else {
                // 실패 행이 있어 전체 미반영 (원자적 처리)
                result.success = false;
                result.message = String.format("실패 %d건이 있어 반영되지 않았습니다. 오류를 수정 후 다시 업로드하세요.", failed);
            }
        } catch (Exception e) {
            result.success = false;
            result.message = "엑셀 업로드 실패: " + e.getMessage();
            log.error("[CmsBillingController] 엑셀 업로드 실패", e);
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

    /** ERP 미수금 후보 조회 (모달 목록) — INSERT 안 함 */
    @GetMapping("/erp-preview")
    public AjaxResult erpPreview(
            @RequestParam("billing_ym") String billingYm,
            @RequestParam(value = "name_type", required = false) String nameType,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        try {
            result.data = cmsBillingService.previewErpBilling(billingYm, nameType);
        } catch (Exception e) {
            result.success = false;
            result.message = e.getMessage();
        }
        return result;
    }

    /** ERP 미수금 선택분으로 청구 생성 — 출금신청일(send_date) 사용자 지정 */
    @PostMapping("/erp-create")
    public AjaxResult erpCreate(
            @RequestParam("billing_ym") String billingYm,
            @RequestParam("send_date")  String sendDate,
            @RequestParam("keys")       String keysStr,
            @RequestParam(value = "deduct_date", required = false) String deductDate,
            @RequestParam(value = "name_type",   required = false) String nameType,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();

        List<String> selectedKeys = java.util.Arrays.stream(keysStr.split(","))
                .map(String::trim).filter(s -> !s.isEmpty())
                .collect(java.util.stream.Collectors.toList());

        try {
            result.data = cmsBillingService.createErpBilling(
                    billingYm, sendDate, selectedKeys, deductDate, user.getUsername(), nameType);
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

    /** 통장기재내용 접미어 저장 (전송 전 건만) */
    /** 검색조건 전체의 청구 id 목록 (그리드 전체선택용, PENDING 만) */
    @GetMapping("/ids")
    public AjaxResult billingIds(
            @RequestParam(value = "billing_ym",     required = false) String billingYm,
            @RequestParam(value = "send_date_from", required = false) String sendDateFrom,
            @RequestParam(value = "send_date_to",   required = false) String sendDateTo,
            @RequestParam(value = "member_name",    required = false) String memberName,
            @RequestParam(value = "status",         required = false) String status,
            @RequestParam(value = "deduct_type",    required = false) String deductType) {
        AjaxResult result = new AjaxResult();
        result.data = cmsBillingService.getBillingIds(
                billingYm, sendDateFrom, sendDateTo, memberName, status, deductType);
        return result;
    }

    @PostMapping("/change-print-suffix")
    public AjaxResult changePrintSuffix(
            @RequestParam String ids,
            @RequestParam(required = false) String print_suffix) {
        AjaxResult result = new AjaxResult();
        try {
            result.data = cmsBillingService.changePrintSuffix(ids, print_suffix);
        } catch (IllegalArgumentException e) {
            result.success = false;
            result.message = e.getMessage();
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