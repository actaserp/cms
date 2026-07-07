package mes.app.cms.service;

import lombok.extern.slf4j.Slf4j;
import mes.app.common.TenantContext;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class CmsBillingService {

    @Autowired
    SqlRunner sqlRunner;

    @Autowired
    CmsHolidayService cmsHolidayService;

    @Autowired
    private CmsMemberService cmsMemberService;

    /** 청구 목록 조회 (페이징) */
    public Map<String, Object> getBillingList(String billingYm, String sendDateFrom, String sendDateTo,
                                              String memberName, String status, String deductType,
                                              int page, int size) {
        String spjangcd = TenantContext.get();
        var param = new org.springframework.jdbc.core.namedparam.MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);
        param.addValue("deductType", deductType != null ? deductType : "EB");

        String baseWhere =
                "  FROM cms_billing b" +
                        "  LEFT JOIN cms_bank_code bc ON bc.bank_code = b.bank_code" +
                        "  LEFT JOIN cms_member m ON m.id = b.member_id" +
                        "  WHERE b.spjangcd    = :spjangcd" +
                        "    AND b.deduct_type = :deductType";

        String filters = "";
        if (StringUtils.hasText(sendDateFrom) && StringUtils.hasText(sendDateTo)) {
            filters += " AND b.send_date BETWEEN :sendDateFrom AND :sendDateTo";
            param.addValue("sendDateFrom", sendDateFrom);
            param.addValue("sendDateTo",   sendDateTo);
        } else if (StringUtils.hasText(sendDateFrom)) {
            filters += " AND b.send_date >= :sendDateFrom";
            param.addValue("sendDateFrom", sendDateFrom);
        } else if (StringUtils.hasText(sendDateTo)) {
            filters += " AND b.send_date <= :sendDateTo";
            param.addValue("sendDateTo", sendDateTo);
        }
        if (StringUtils.hasText(memberName)) {
            filters += " AND b.member_name LIKE '%' || :memberName || '%'";
            param.addValue("memberName", memberName);
        }
        if (StringUtils.hasText(status)) {
            filters += " AND b.status = :status";
            param.addValue("status", status);
        } else {
            filters += " AND b.status IN ('PENDING', 'REQUESTED', 'CANCEL')";
        }

        Map<String, Object> aggRow = sqlRunner.getRow(
                "SELECT COUNT(*) AS cnt, COALESCE(SUM(b.billing_amount),0) AS total_amount" +
                        baseWhere + filters, param);
        long totalCount  = aggRow != null ? ((Number) aggRow.get("cnt")).longValue()          : 0L;
        long totalAmount = aggRow != null ? ((Number) aggRow.get("total_amount")).longValue() : 0L;

        String dataSql =
                "SELECT b.id, b.billing_ym, b.billing_seq, b.member_id, b.member_name," +
                        "       m.member_no, m.id_number, b.bank_code, bc.bank_name, b.bank_account," +
                        "       b.account_holder, b.billing_amount, b.deduct_day, b.deduct_date," +
                        "       b.send_date, b.status, b.result_code, b.result_msg, b.result_date," +
                        "       b.memo, b._created, b._modified," +
                        "       CASE WHEN EXISTS (" +
                        "           SELECT 1 FROM cms_billing rb" +
                        "           WHERE rb.spjangcd = b.spjangcd AND rb.id > b.id" +
                        "             AND rb.status NOT IN ('CANCEL', 'FAIL', 'ERROR')" +
                        "             AND ( (b.erp_mis_key IS NOT NULL AND rb.erp_mis_key = b.erp_mis_key)" +
                        "                OR (b.erp_mis_key IS NULL AND rb.member_id = b.member_id" +
                        "                    AND rb.memo LIKE '%불능 / 재청구%' AND rb.deduct_date > b.deduct_date) )" +
                        "       ) THEN 'Y' ELSE 'N' END AS recharged_yn" +
                        baseWhere + filters +
                        " ORDER BY b.billing_seq LIMIT :pgSize OFFSET :pgOffset";

        param.addValue("pgSize",   size);
        param.addValue("pgOffset", (long) page * size);
        List<Map<String, Object>> rows = sqlRunner.getRows(dataSql, param);

        var result = new java.util.HashMap<String, Object>();
        result.put("data", rows);
        result.put("totalCount",  totalCount);
        result.put("totalAmount", totalAmount);
        return result;
    }

    /** 청구 단건 조회 */
    public Map<String, Object> getBilling(Long id) {
        String spjangcd = TenantContext.get();
        var param = new org.springframework.jdbc.core.namedparam.MapSqlParameterSource();
        param.addValue("id", id);
        param.addValue("spjangcd", spjangcd);

        String sql = """
                SELECT b.id
                     , b.billing_ym
                     , b.billing_seq
                     , b.member_id
                     , b.member_name
                     , b.bank_code
                     , bc.bank_name
                     , b.bank_account
                     , b.account_holder
                     , b.billing_amount
                     , b.deduct_day
                     , b.deduct_date
                     , b.send_date
                     , b.status
                     , b.result_code
                     , b.result_msg
                     , b.result_date
                     , b.memo
                FROM cms_billing b
                LEFT JOIN cms_bank_code bc ON bc.bank_code = b.bank_code
                WHERE b.id = :id AND b.spjangcd = :spjangcd
                """;
        return sqlRunner.getRow(sql, param);
    }

    /**
     * 청구 저장 (신규/수정) - 중지 기간 체크 추가됨
     */
    public Long saveBilling(Long id, String billingYm, String memberId,
                            String memberName, String bankCode, String bankAccount,
                            String accountHolder, Long billingAmount,
                            String deductDay, String deductDate,
                            String status, String memo, String deductType, String userId) {
        String spjangcd = TenantContext.get();

        log.info("[CmsBillingService] saveBilling - id:{} memberId:{} deductDate:{}", id, memberId, deductDate);
        // ⭐ 수동 청구: memberId가 있고 신규 저장일 때 중지 기간 체크
        if (id == null && StringUtils.hasText(memberId)) {
            try {
                Long memberIdLong = Long.parseLong(memberId);
                Map<String, Object> member = cmsMemberService.getMember(memberIdLong);

                if (member != null) {
                    PauseCheckResult pauseResult = checkPausePeriod(member, deductDate);
                    if (pauseResult.isPaused) {
                        log.warn("[CmsBillingService] 중지 기간 경고 - memberId: {}, deductDate: {}, pausePeriod: {}",
                                memberId, deductDate, pauseResult.displayText);
                        throw new IllegalStateException(
                                "이 납부자는 현재 중지 기간입니다. (" + pauseResult.displayText + ") " +
                                        "계속 청구하시겠습니까?"
                        );
                    }

                    // 인증 여부 체크 추가
                    String agreeYn = member.get("agree_yn") != null ? member.get("agree_yn").toString() : "N";
                    if (!"Y".equals(agreeYn)) {
                        throw new IllegalStateException("출금이체 동의가 완료되지 않은 납부자입니다.");
                    }
                }
            } catch (NumberFormatException e) {
                log.error("[CmsBillingService] memberId 파싱 오류: {}", memberId);
            }
        }

        var param = new org.springframework.jdbc.core.namedparam.MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);
        param.addValue("billingYm", billingYm);
        param.addValue("memberId", memberId != null ? Long.parseLong(memberId) : null);
        param.addValue("memberName", memberName);
        param.addValue("bankCode", bankCode);
        param.addValue("bankAccount", bankAccount);
        param.addValue("accountHolder", accountHolder);
        param.addValue("billingAmount", billingAmount);
        param.addValue("deductDay", deductDay);
        param.addValue("deductDate", deductDate);
        param.addValue("status", status != null ? status : "PENDING");
        param.addValue("memo", memo);
        param.addValue("deductType", deductType != null ? deductType : "EB");
        param.addValue("userId", userId);
        String effectiveDeductType = deductType != null ? deductType : "EB";
        if (StringUtils.hasText(deductDate)) {
            param.addValue("sendDate", calcSendDate(deductDate, effectiveDeductType));
        } else {
            param.addValue("sendDate", null);
        }

        if (id == null) {
            // 중복 청구 체크
//            if (StringUtils.hasText(memberId) && StringUtils.hasText(deductDate)) {
//                List<Map<String, Object>> dup = sqlRunner.getRows(/* skip_tenant_check */
//                        """
//                        SELECT 1 FROM cms_billing
//                        WHERE spjangcd  = :spjangcd
//                          AND member_id = :memberId
//                          AND deduct_date = :deductDate
//                          AND status NOT IN ('CANCEL', 'FAIL', 'ERROR')
//                        """,
//                        new org.springframework.jdbc.core.namedparam.MapSqlParameterSource()
//                                .addValue("spjangcd",   spjangcd)
//                                .addValue("memberId",   Long.parseLong(memberId))
//                                .addValue("deductDate", deductDate));
//                if (!dup.isEmpty()) {
//                    log.warn("[saveBilling] 중복 청구 차단 memberId={} deductDate={}", memberId, deductDate);
//                    throw new IllegalStateException("동일한 출금일에 이미 청구가 존재합니다.");
//                }
//            }

            // 청구번호 채번
            String billingSeq = generateBillingSeq(spjangcd, billingYm);
            param.addValue("billingSeq", billingSeq);

            String sql = """
                INSERT INTO cms_billing (
                    spjangcd, billing_ym, billing_seq,
                    member_id, member_name, bank_code, bank_account, account_holder,
                    billing_amount, deduct_day, deduct_date, send_date,
                    deduct_type, status, memo,
                    _creater_id, _created, _modifier_id, _modified
                ) VALUES (
                    :spjangcd, :billingYm, :billingSeq,
                    :memberId, :memberName, :bankCode, :bankAccount, :accountHolder,
                    :billingAmount, :deductDay, :deductDate, :sendDate,
                    :deductType, :status, :memo,
                    :userId, NOW(), :userId, NOW()
                ) RETURNING id
                """;
            Map<String, Object> row = sqlRunner.getRow(sql, param);
            if (row == null) return null;
            return ((Number) row.get("id")).longValue();
        } else {
            param.addValue("id", id);

            // 수정 시: 기존 청구신청일이 있으면 그대로 유지 (자동 재계산 금지)
            Map<String, Object> cur = sqlRunner.getRow(
                    "SELECT send_date FROM cms_billing WHERE id = :id AND spjangcd = :spjangcd",
                    new org.springframework.jdbc.core.namedparam.MapSqlParameterSource()
                            .addValue("id", id).addValue("spjangcd", spjangcd));
            if (cur != null && cur.get("send_date") != null
                    && StringUtils.hasText(cur.get("send_date").toString())) {
                param.addValue("sendDate", cur.get("send_date").toString());
            }

            String sql = """
                UPDATE cms_billing SET
                    member_name    = :memberName,
                    bank_code      = :bankCode,
                    bank_account   = :bankAccount,
                    account_holder = :accountHolder,
                    billing_amount = :billingAmount,
                    deduct_day     = :deductDay,
                    deduct_date    = :deductDate,
                    send_date      = :sendDate,
                    status         = :status,
                    memo           = :memo,
                    _modifier_id   = :userId,
                    _modified      = NOW()
                WHERE id = :id AND spjangcd = :spjangcd
                """;
            int affected = sqlRunner.execute(sql, param);
            return affected > 0 ? id : null;
        }
    }

    /**
     * 청구 강제 저장 (중지 기간이어도 진행)
     * - 사용자가 경고 후 "확인" 클릭했을 때 호출
     * - 메모에 "[강제 청구]" 추가
     */
    public Long saveBillingForce(Long id, String billingYm, String memberId,
                                 String memberName, String bankCode, String bankAccount,
                                 String accountHolder, Long billingAmount,
                                 String deductDay, String deductDate,
                                 String status, String memo, String deductType, String userId) {
        // 중지 기간 체크 스킵
        // 메모에 "[강제 청구]" 추가
        String forceMemo = "[강제 청구] " + (StringUtils.hasText(memo) ? memo : "");

        log.warn("[CmsBillingService] 강제 청구 생성 - memberId: {}, deductDate: {}, memo: {}",
                memberId, deductDate, forceMemo);

        // 기존 saveBilling 로직 실행 (중지 기간 체크 없음)
        return saveBillingWithoutPauseCheck(id, billingYm, memberId, memberName, bankCode,
                bankAccount, accountHolder, billingAmount, deductDay, deductDate,
                status, forceMemo, deductType, userId);
    }

    /**
     * 청구 저장 (중지 기간 체크 제외 버전)
     * - saveBillingForce()에서만 호출
     */
    private Long saveBillingWithoutPauseCheck(Long id, String billingYm, String memberId,
                                              String memberName, String bankCode, String bankAccount,
                                              String accountHolder, Long billingAmount,
                                              String deductDay, String deductDate,
                                              String status, String memo, String deductType, String userId) {
        String spjangcd = TenantContext.get();
        var param = new org.springframework.jdbc.core.namedparam.MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);
        param.addValue("billingYm", billingYm);
        param.addValue("memberId", memberId != null ? Long.parseLong(memberId) : null);
        param.addValue("memberName", memberName);
        param.addValue("bankCode", bankCode);
        param.addValue("bankAccount", bankAccount);
        param.addValue("accountHolder", accountHolder);
        param.addValue("billingAmount", billingAmount);
        param.addValue("deductDay", deductDay);
        param.addValue("deductDate", deductDate);
        param.addValue("status", status != null ? status : "PENDING");
        param.addValue("memo", memo);
        param.addValue("deductType", deductType != null ? deductType : "EB");
        param.addValue("userId", userId);
        String effectiveDeductType = deductType != null ? deductType : "EB";
        if (StringUtils.hasText(deductDate)) {
            param.addValue("sendDate", calcSendDate(deductDate, effectiveDeductType));
        } else {
            param.addValue("sendDate", null);
        }

        if (id == null) {
            // 중복 청구 체크
//            if (StringUtils.hasText(memberId) && StringUtils.hasText(deductDate)) {
//                List<Map<String, Object>> dup = sqlRunner.getRows(/* skip_tenant_check */
//                        """
//                        SELECT 1 FROM cms_billing
//                        WHERE spjangcd  = :spjangcd
//                          AND member_id = :memberId
//                          AND deduct_date = :deductDate
//                          AND status NOT IN ('CANCEL', 'FAIL', 'ERROR')
//                        """,
//                        new org.springframework.jdbc.core.namedparam.MapSqlParameterSource()
//                                .addValue("spjangcd",   spjangcd)
//                                .addValue("memberId",   Long.parseLong(memberId))
//                                .addValue("deductDate", deductDate));
//                if (!dup.isEmpty()) {
//                    log.warn("[saveBillingWithoutPauseCheck] 중복 청구 차단 memberId={} deductDate={}", memberId, deductDate);
//                    throw new IllegalStateException("동일한 출금일에 이미 청구가 존재합니다.");
//                }
//            }

            String billingSeq = generateBillingSeq(spjangcd, billingYm);
            param.addValue("billingSeq", billingSeq);

            String sql = """
                INSERT INTO cms_billing (
                    spjangcd, billing_ym, billing_seq,
                    member_id, member_name, bank_code, bank_account, account_holder,
                    billing_amount, deduct_day, deduct_date, send_date,
                    deduct_type, status, memo,
                    _creater_id, _created, _modifier_id, _modified
                ) VALUES (
                    :spjangcd, :billingYm, :billingSeq,
                    :memberId, :memberName, :bankCode, :bankAccount, :accountHolder,
                    :billingAmount, :deductDay, :deductDate, :sendDate,
                    :deductType, :status, :memo,
                    :userId, NOW(), :userId, NOW()
                ) RETURNING id
                """;
            Map<String, Object> row = sqlRunner.getRow(sql, param);
            if (row == null) return null;
            return ((Number) row.get("id")).longValue();
        } else {
            param.addValue("id", id);

            // 수정 시: 기존 청구신청일이 있으면 그대로 유지 (자동 재계산 금지)
            Map<String, Object> cur = sqlRunner.getRow(
                    "SELECT send_date FROM cms_billing WHERE id = :id AND spjangcd = :spjangcd",
                    new org.springframework.jdbc.core.namedparam.MapSqlParameterSource()
                            .addValue("id", id).addValue("spjangcd", spjangcd));
            if (cur != null && cur.get("send_date") != null
                    && StringUtils.hasText(cur.get("send_date").toString())) {
                param.addValue("sendDate", cur.get("send_date").toString());
            }

            String sql = """
                UPDATE cms_billing SET
                    member_name    = :memberName,
                    bank_code      = :bankCode,
                    bank_account   = :bankAccount,
                    account_holder = :accountHolder,
                    billing_amount = :billingAmount,
                    deduct_day     = :deductDay,
                    deduct_date    = :deductDate,
                    send_date      = :sendDate,
                    status         = :status,
                    memo           = :memo,
                    _modifier_id   = :userId,
                    _modified      = NOW()
                WHERE id = :id AND spjangcd = :spjangcd
                """;
            int affected = sqlRunner.execute(sql, param);
            return affected > 0 ? id : null;
        }
    }

    /** 청구 삭제 */
    public boolean deleteBilling(Long id) {
        String spjangcd = TenantContext.get();
        var param = new org.springframework.jdbc.core.namedparam.MapSqlParameterSource();
        param.addValue("id", id);
        param.addValue("spjangcd", spjangcd);

        String sql = """
                DELETE FROM cms_billing WHERE id = :id AND spjangcd = :spjangcd AND status = 'PENDING'
                """;
        return sqlRunner.execute(sql, param) > 0;
    }

    // ──────────────────────────────────────────────────────────────────
    // ⭐ 중지 기간 체크 관련 메서드 (새로 추가됨)
    // ──────────────────────────────────────────────────────────────────

    /**
     * 중지 기간 체크 결과 클래스
     */
    private static class PauseCheckResult {
        boolean isPaused;
        String displayText;  // UI에 표시할 메시지

        PauseCheckResult(boolean isPaused, String displayText) {
            this.isPaused = isPaused;
            this.displayText = displayText;
        }
    }

    /**
     * 중지 기간 체크 (수동 청구용)
     * - 출금일이 중지 기간 범위 내인지 확인
     * - 중지 기간이면 PauseCheckResult(true, "중지기간: xxx ~ yyy") 반환
     */
    private PauseCheckResult checkPausePeriod(Map<String, Object> member, String deductDate) {
        String pauseStartDate = objToStr(member.get("pause_start_date"));
        String pauseEndDate   = objToStr(member.get("pause_end_date"));

        if (!StringUtils.hasText(pauseStartDate) || !StringUtils.hasText(pauseEndDate)) {
            return new PauseCheckResult(false, "");
        }

        try {
            if (!StringUtils.hasText(deductDate)) return new PauseCheckResult(false, "");
            LocalDate deductLocalDate = parseFlexDate(deductDate);
            LocalDate start = parseFlexDate(pauseStartDate);
            LocalDate end   = parseFlexDate(pauseEndDate);

            boolean isPaused = !deductLocalDate.isBefore(start) && !deductLocalDate.isAfter(end);

            if (isPaused) {
                String displayText = String.format(
                        "중지기간: %s ~ %s",
                        start.format(DateTimeFormatter.ofPattern("yyyy.MM.dd")),
                        end.format(DateTimeFormatter.ofPattern("yyyy.MM.dd"))
                );
                return new PauseCheckResult(true, displayText);
            }

            return new PauseCheckResult(false, "");
        } catch (Exception e) {
            log.error("[CmsBillingService] 중지 기간 파싱 오류 - pauseStartDate: {}, pauseEndDate: {}",
                    pauseStartDate, pauseEndDate, e);
            return new PauseCheckResult(false, "");
        }
    }

    /**
     * 해당 청구년월이 납부자의 중지 기간과 겹치는지 확인 (자동생성용)
     * "오늘"이 아니라 "청구 대상 월" 기준으로 판단
     */
    private boolean isPausedInBillingMonth(Map<String, Object> member, YearMonth billingYm) {
        String pauseStartStr = objToStr(member.get("pause_start_date"));
        String pauseEndStr   = objToStr(member.get("pause_end_date"));

        if (!StringUtils.hasText(pauseStartStr) || !StringUtils.hasText(pauseEndStr)) {
            return false;
        }

        try {
            LocalDate pauseStart  = parseFlexDate(pauseStartStr);
            LocalDate pauseEnd    = parseFlexDate(pauseEndStr);
            LocalDate billingFirst = billingYm.atDay(1);
            LocalDate billingLast  = billingYm.atEndOfMonth();

            return !pauseStart.isAfter(billingLast) && !pauseEnd.isBefore(billingFirst);
        } catch (Exception e) {
            log.error("[CmsBillingService] 중지 기간 파싱 오류 - member: {}, error: {}",
                    member.get("member_name"), e.getMessage());
            return false;
        }
    }

    /** DB 반환값이 java.sql.Date, LocalDate, String 어느 타입이든 안전하게 "yyyyMMdd" String 변환 */
    private String objToStr(Object val) {
        if (val == null) return "";
        if (val instanceof java.sql.Date) {
            return ((java.sql.Date) val).toLocalDate().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        }
        if (val instanceof LocalDate) {
            return ((LocalDate) val).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        }
        return val.toString().trim();
    }

    /** "yyyyMMdd" 또는 "yyyy-MM-dd" 두 형식 모두 파싱 */
    private LocalDate parseFlexDate(String s) {
        s = s.trim();
        if (s.contains("-")) {
            return LocalDate.parse(s, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        }
        return LocalDate.parse(s, DateTimeFormatter.ofPattern("yyyyMMdd"));
    }

    /** 단건 수동등록용 billing_seq 생성 */
    private String generateBillingSeq(String spjangcd, String billingYm) {
        int seq = getNextBillingSeqNo(spjangcd, billingYm);
        return billingYm + "-" + String.format("%04d", seq);
    }

    private String calcSendDate(String deductDate, String deductType) {
        if ("EC".equals(deductType)) return deductDate;
        return cmsHolidayService.getPrevBusinessDay(
                LocalDate.parse(deductDate, DateTimeFormatter.ofPattern("yyyyMMdd"))
                        .minusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMdd")));
    }

    /** 청구 자동생성 */
    @Transactional
    public Map<String, Object> generateBilling(String billingYm, String deductType, String userId) {
        String spjangcd = TenantContext.get();

        // 청구년월 → 해당 월의 첫날/마지막날
        YearMonth ym = YearMonth.parse(billingYm, DateTimeFormatter.ofPattern("yyyyMM"));
        String firstDay = ym.atDay(1).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String lastDay  = ym.atEndOfMonth().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        // cycle_months 비교용 월 (정수 문자열: "4", "12" 등)
        String monthStr = String.valueOf(ym.getMonthValue());

        var param = new org.springframework.jdbc.core.namedparam.MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);
        param.addValue("billingYm", billingYm);
        param.addValue("firstDay", firstDay);
        param.addValue("lastDay", lastDay);
        param.addValue("monthStr", monthStr);
        param.addValue("deductType", deductType != null ? deductType : "EB");

        // 자동생성 대상 납부자 조회
        String memberSql = """
                SELECT m.id
                     , m.member_name
                     , m.bank_code
                     , m.bank_account
                     , m.account_holder
                     , m.deduct_amount
                     , m.deduct_day
                     , m.pause_start_date
                     , m.pause_end_date
                FROM cms_member m
                WHERE m.spjangcd     = :spjangcd
                  AND m.status       = 'ACTIVE'
                  AND m.agree_yn     = 'Y'
                  AND m.start_date  <= :lastDay
                  AND m.end_date    >= :firstDay
                  AND (
                      m.cycle_type = 'REGULAR'
                      OR (m.cycle_type = 'IRREGULAR' AND :monthStr = ANY(STRING_TO_ARRAY(m.cycle_months, ',')))
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM cms_billing b
                      WHERE b.member_id   = m.id
                        AND b.billing_ym  = :billingYm
                        AND b.spjangcd    = :spjangcd
                        AND b.deduct_type = :deductType
                  )
                ORDER BY m.id
                """;

        List<Map<String, Object>> members = sqlRunner.getRows(memberSql, param);

        if (members.isEmpty()) {
            Map<String, Object> result = new HashMap<>();
            result.put("count", 0);
            return result;
        }

        // 현재 최대 seq 조회 → 채번 시작값 결정
        int nextSeq = getNextBillingSeqNo(spjangcd, billingYm);

        // 건별 INSERT
        int count = 0;
        int skippedCount = 0;
        java.time.LocalDate today = java.time.LocalDate.now();
        int nowHour = java.time.LocalTime.now().getHour();
        String todayStr = today.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String tomorrowStr = today.plusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String effectiveDeductType = deductType != null ? deductType : "EB";

        for (Map<String, Object> m : members) {
            String deductDay = (String) m.get("deduct_day");
            String deductDate = "99".equals(deductDay) ? lastDay : billingYm + deductDay;

            if (deductDay == null || deductDay.isEmpty()) {
                skippedCount++;
                continue;
            }

            deductDate = cmsHolidayService.getNextBusinessDay(deductDate);

            // 오늘 이전 날짜 스킵
            if (deductDate.compareTo(todayStr) < 0) { skippedCount++; continue; }
            if ("EB".equals(effectiveDeductType)) {
                String deadlineDay = cmsHolidayService.getPrevBusinessDay(
                        LocalDate.parse(deductDate, DateTimeFormatter.ofPattern("yyyyMMdd"))
                                .minusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMdd")));
                if (deadlineDay.equals(todayStr) && nowHour >= 17) { skippedCount++; continue; }
            }
            if ("EC".equals(effectiveDeductType) && deductDate.equals(todayStr) && nowHour >= 11) { skippedCount++; continue; }

            // 중지 기간 체크 — 청구년월 기준으로 판단
            if (isPausedInBillingMonth(m, ym)) {
                log.info("[generateBilling] 중지 기간 → 청구 생성 스킵 - memberId: {}", m.get("id"));
                skippedCount++;
                continue;
            }

            String billingSeq = billingYm + "-" + String.format("%04d", nextSeq++);

            var ip = new org.springframework.jdbc.core.namedparam.MapSqlParameterSource();
            ip.addValue("spjangcd",      spjangcd);
            ip.addValue("billingYm",     billingYm);
            ip.addValue("billingSeq",    billingSeq);
            ip.addValue("memberId",      ((Number) m.get("id")).longValue());
            ip.addValue("memberName",    m.get("member_name"));
            ip.addValue("bankCode",      m.get("bank_code"));
            ip.addValue("bankAccount",   m.get("bank_account"));
            ip.addValue("accountHolder", m.get("account_holder"));
            ip.addValue("billingAmount", m.get("deduct_amount"));
            ip.addValue("deductDay",     deductDay);
            ip.addValue("deductDate",    deductDate);
            ip.addValue("deductType",    deductType != null ? deductType : "EB");
            ip.addValue("userId",        userId);
            ip.addValue("sendDate",      calcSendDate(deductDate, deductType != null ? deductType : "EB"));

            String insertSql = """
                    INSERT INTO cms_billing (
                        spjangcd, billing_ym, billing_seq,
                        member_id, member_name, bank_code, bank_account, account_holder,
                        billing_amount, deduct_day, deduct_date, send_date,
                        deduct_type, status, _creater_id, _created, _modifier_id, _modified
                    ) VALUES (
                        :spjangcd, :billingYm, :billingSeq,
                        :memberId, :memberName, :bankCode, :bankAccount, :accountHolder,
                        :billingAmount, :deductDay, :deductDate, :sendDate,
                        :deductType, 'PENDING', :userId, NOW(), :userId, NOW()
                    )
                    """;
            sqlRunner.execute(insertSql, ip);
            count++;
        }

        Map<String, Object> result = new HashMap<>();
        result.put("count", count);
        result.put("skippedCount", skippedCount);
        return result;
    }

    /** 청구 취소 (PENDING → CANCEL) */
    public int cancelBilling(List<Long> ids, String userId) {
        String spjangcd = TenantContext.get();
        var param = new org.springframework.jdbc.core.namedparam.MapSqlParameterSource();
        param.addValue("ids", ids);
        param.addValue("spjangcd", spjangcd);
        param.addValue("userId", userId);

        String sql = """
                UPDATE cms_billing SET
                    status       = 'CANCEL',
                    _modifier_id = :userId,
                    _modified    = NOW()
                WHERE id IN (:ids)
                  AND spjangcd = :spjangcd
                  AND status   = 'PENDING'
                """;
        return sqlRunner.execute(sql, param);
    }

    /** 수납결과 조회 (billing_ym 필수, result_date/status/member_name/deduct_type 선택) */
    public Map<String, Object> getBillingResultList(String billingYm, String resultDate, String status,
                                                    String memberName, String deductType, int page, int size) {
        String spjangcd = TenantContext.get();
        var param = new org.springframework.jdbc.core.namedparam.MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);
        param.addValue("billingYm", billingYm);
        param.addValue("deductType", deductType != null ? deductType : "EB");

        String baseWhere =
                "  FROM cms_billing b" +
                        "  LEFT JOIN cms_bank_code bc ON bc.bank_code = b.bank_code" +
                        "  LEFT JOIN cms_member m ON m.id = b.member_id" +
                        "  WHERE b.spjangcd = :spjangcd" +
                        "    AND LEFT(COALESCE(b.deduct_date), 6) = :billingYm" +
                        "    AND b.deduct_type = :deductType";

        String filters = "";
        if (StringUtils.hasText(resultDate)) {
            filters += " AND (b.result_date = :resultDate OR (b.status = 'FAIL' AND b.deduct_date = :resultDate))";
            param.addValue("resultDate", resultDate);
        }
        if (StringUtils.hasText(status)) {
            filters += " AND b.status = :status";
            param.addValue("status", status);
        } else {
            filters += " AND b.status IN ('SUCCESS', 'FAIL')";
        }
        if (StringUtils.hasText(memberName)) {
            filters += " AND b.member_name LIKE '%' || :memberName || '%'";
            param.addValue("memberName", memberName);
        }

        Map<String, Object> aggRow = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT COUNT(*) AS cnt," +
                        "       COALESCE(SUM(b.billing_amount),0) AS total_amount," +
                        "       COALESCE(SUM(CASE WHEN b.status='SUCCESS' THEN b.fee_request+b.fee_success" +
                        "                         WHEN b.status='FAIL'    THEN b.fee_request ELSE 0 END),0) AS total_fee," +
                        "       COUNT(CASE WHEN b.status='SUCCESS' THEN 1 END) AS success_count," +
                        "       COUNT(CASE WHEN b.status='FAIL'    THEN 1 END) AS fail_count," +
                        "       COALESCE(SUM(CASE WHEN b.status='SUCCESS' THEN b.billing_amount ELSE 0 END),0) AS success_billing_amount," +
                        "       COALESCE(SUM(CASE WHEN b.status='FAIL'    THEN b.billing_amount ELSE 0 END),0) AS fail_amount," +
                        "       COALESCE(SUM(CASE WHEN b.status='SUCCESS'" +
                        "                         THEN b.billing_amount-(b.fee_request+b.fee_success) ELSE 0 END),0) AS success_deposit_amount" +
                        baseWhere + filters, param);
        long totalCount  = aggRow != null ? ((Number) aggRow.get("cnt")).longValue()          : 0L;
        long totalAmount = aggRow != null ? ((Number) aggRow.get("total_amount")).longValue() : 0L;
        long totalFee    = aggRow != null ? ((Number) aggRow.get("total_fee")).longValue()    : 0L;
        long successCount          = aggRow != null ? ((Number) aggRow.get("success_count")).longValue()          : 0L;
        long failCount             = aggRow != null ? ((Number) aggRow.get("fail_count")).longValue()             : 0L;
        long successBillingAmount  = aggRow != null ? ((Number) aggRow.get("success_billing_amount")).longValue() : 0L;
        long failAmount            = aggRow != null ? ((Number) aggRow.get("fail_amount")).longValue()            : 0L;
        long successDepositAmount  = aggRow != null ? ((Number) aggRow.get("success_deposit_amount")).longValue() : 0L;

        String dataSql =
                "SELECT b.id, b.billing_seq, b.member_name, m.id_number," +
                        "       b.bank_code, bc.bank_name, b.bank_account, b.billing_amount," +
                        "       b.deduct_date, b.status, b.result_code, b.result_msg, b.result_date," +
                        "       b.fee_request, b.fee_success," +
                        "       CASE WHEN b.status='SUCCESS' THEN b.fee_request+b.fee_success" +
                        "            WHEN b.status='FAIL'    THEN b.fee_request ELSE 0 END AS fee_total," +
                        "       CASE WHEN EXISTS (SELECT 1 FROM cms_billing rb WHERE rb.spjangcd=b.spjangcd" +
                        "              AND rb.id > b.id AND rb.status NOT IN ('CANCEL','FAIL','ERROR')" +
                        "              AND ( (b.erp_mis_key IS NOT NULL AND rb.erp_mis_key = b.erp_mis_key)" +
                        "                 OR (b.erp_mis_key IS NULL AND rb.member_id=b.member_id" +
                        "                     AND rb.memo LIKE '%불능 / 재청구%' AND rb.deduct_date > b.deduct_date) )" +
                        "              ) THEN 'Y' ELSE 'N' END AS recharged_yn" +
                        baseWhere + filters + " ORDER BY b.billing_seq LIMIT :pgSize OFFSET :pgOffset";

        param.addValue("pgSize",   size);
        param.addValue("pgOffset", (long) page * size);
        List<Map<String, Object>> rows = sqlRunner.getRows(dataSql, param);

        var result = new java.util.HashMap<String, Object>();
        result.put("data", rows);
        result.put("totalCount",  totalCount);
        result.put("totalAmount", totalAmount);
        result.put("totalFee",    totalFee);
        // ⭐ 전체(검색조건 전체) 기준 성공/실패 집계 — 상단 요약 카드용
        result.put("successCount",        successCount);
        result.put("failCount",           failCount);
        result.put("successAmount",       successDepositAmount);   // 성공 입금금액(수수료 차감)
        result.put("failAmount",          failAmount);             // 실패 청구금액
        result.put("successBillingAmount", successBillingAmount);  // 수납률 분자용(성공 청구금액)
        return result;
    }

    /** 불능 건 재청구 — FAIL 상태 건을 납부자 현재 정보 기준으로 새 PENDING 생성 */
    @Transactional
    // 778행 시그니처 변경 — deductType 파라미터 추가
    public Map<String, Object> rechargeBilling(List<Long> ids, List<String> deductDates,
                                               List<String> deductTypes, String userId) {
        String spjangcd = TenantContext.get();
        int count = 0;

        for (int i = 0; i < ids.size(); i++) {
            Long id = ids.get(i);
            String newDeductDate = deductDates.size() > i ? deductDates.get(i) : null;
            // 건별 EB/EC 구분 (없으면 원본 타입 상속)
            String newDeductType = (deductTypes != null && deductTypes.size() > i)
                    ? deductTypes.get(i) : null;
            var pOrig = new org.springframework.jdbc.core.namedparam.MapSqlParameterSource();
            pOrig.addValue("id", id);
            pOrig.addValue("spjangcd", spjangcd);

            Map<String, Object> orig = sqlRunner.getRow("""
                SELECT billing_ym, member_id, member_name,
                       bank_code, bank_account, account_holder,
                       billing_amount, deduct_day, deduct_date, result_code, deduct_type,
                       erp_mis_key
                FROM cms_billing
                WHERE id = :id AND spjangcd = :spjangcd AND status IN ('FAIL', 'ERROR')
                """, pOrig);
            if (orig == null) continue;

            String billingYm     = (String) orig.get("billing_ym");
            Object memberIdObj   = orig.get("member_id");
            String memberName    = (String) orig.get("member_name");
            String bankCode      = (String) orig.get("bank_code");
            String bankAccount   = (String) orig.get("bank_account");
            String accountHolder = (String) orig.get("account_holder");
            Object billingAmount = orig.get("billing_amount");
            String origDeductType = (String) orig.get("deduct_type");
            String deductType     = StringUtils.hasText(newDeductType) ? newDeductType
                    : (origDeductType != null ? origDeductType : "EB");
            String misKey         = (String) orig.get("erp_mis_key");
            String deductDay     = (String) orig.get("deduct_day");
            String deductDate    = (String) orig.get("deduct_date");
            String origDeductDate = deductDate;
            String resultCode    = (String) orig.get("result_code");

            // 납부자 현재 정보로 은행/계좌/금액 갱신
            if (memberIdObj != null) {
                var pMember = new org.springframework.jdbc.core.namedparam.MapSqlParameterSource();
                pMember.addValue("id", ((Number) memberIdObj).longValue());
                pMember.addValue("spjangcd", spjangcd);
                Map<String, Object> member = sqlRunner.getRow("""
                    SELECT bank_code, bank_account, account_holder, deduct_amount, deduct_day
                    FROM cms_member WHERE id = :id AND spjangcd = :spjangcd
                    """, pMember);
                if (member != null) {
                    bankCode      = (String) member.get("bank_code");
                    bankAccount   = (String) member.get("bank_account");
                    accountHolder = (String) member.get("account_holder");
//                    if (member.get("deduct_amount") != null) billingAmount = member.get("deduct_amount");
//                    if (member.get("deduct_day")    != null) deductDay     = (String) member.get("deduct_day");
                }
            }

            // deduct_date가 오늘 이전이면 오늘(EC) 또는 내일(EB)로 변경
            String todayStr    = java.time.LocalDate.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
            String tomorrowStr = java.time.LocalDate.now().plusDays(1).format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
            if (StringUtils.hasText(newDeductDate)) {
                deductDate = newDeductDate;
            } else if (deductDate != null && deductDate.compareTo(todayStr) < 0) {
                String rawDate = "EB".equals(deductType) ? tomorrowStr : todayStr;
                deductDate = cmsHolidayService.getNextBusinessDay(rawDate);
            }

            // 재청구 청구년월 = 실제 출금월(deduct_date 기준). CMS는 회계귀속이 아니라
            // 출금 실행월로 관리하므로, 원본 미수월이 아닌 출금월로 잡아 해당 월 화면·전송에 노출되게 함.
            // 원본 미수와의 연결은 erp_mis_key로 유지됨.
            if (deductDate != null && deductDate.length() >= 6) {
                billingYm = deductDate.substring(0, 6);
            }

            String memo = origDeductDate + " 불능 / 재청구";
            String billingSeq = generateBillingSeq(spjangcd, billingYm);

            var pIns = new org.springframework.jdbc.core.namedparam.MapSqlParameterSource();
            pIns.addValue("spjangcd",      spjangcd);
            pIns.addValue("billingYm",     billingYm);
            pIns.addValue("billingSeq",    billingSeq);
            pIns.addValue("memberId",      memberIdObj != null ? ((Number) memberIdObj).longValue() : null);
            pIns.addValue("memberName",    memberName);
            pIns.addValue("bankCode",      bankCode);
            pIns.addValue("bankAccount",   bankAccount);
            pIns.addValue("accountHolder", accountHolder);
            pIns.addValue("billingAmount", billingAmount);
            pIns.addValue("deductDay",     deductDay);
            pIns.addValue("deductDate",    deductDate);
            pIns.addValue("deductType",    deductType != null ? deductType : "EB");
            pIns.addValue("memo",          memo);
            pIns.addValue("userId",        userId);
            // 재청구 출금신청일 = 생성일(오늘). 출금일 역산이 아니라 만든 날로 신청.
            String todaySendDate = java.time.LocalDate.now()
                    .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
            pIns.addValue("sendDate", todaySendDate);
            pIns.addValue("erpMisKey", misKey);

            sqlRunner.execute("""
                INSERT INTO cms_billing (
                    spjangcd, billing_ym, billing_seq,
                    member_id, member_name, bank_code, bank_account, account_holder,
                    billing_amount, deduct_day, deduct_date, send_date,
                    deduct_type, status, memo, erp_mis_key,
                    _creater_id, _created, _modifier_id, _modified
                ) VALUES (
                    :spjangcd, :billingYm, :billingSeq,
                    :memberId, :memberName, :bankCode, :bankAccount, :accountHolder,
                    :billingAmount, :deductDay, :deductDate, :sendDate,
                    :deductType, 'PENDING', :memo, :erpMisKey,
                    :userId, NOW(), :userId, NOW()
                )
                """, pIns);
            count++;
        }

        Map<String, Object> result = new HashMap<>();
        result.put("count", count);
        return result;
    }

    /**
     * EB파일 전송 후 PENDING → REQUESTED 배치 전환 (스케줄러 전용 — skip_tenant_check)
     * billingIds 전체를 단일 UPDATE로 처리
     */
    // REQUESTED 전환 시 (재시도 성공)
    public int updateStatusToRequested(List<Long> billingIds, Long fileId, int feeRequest) {
        if (billingIds == null || billingIds.isEmpty()) return 0;
        var param = new MapSqlParameterSource();
        param.addValue("ids", billingIds);
        param.addValue("fileId", fileId);
        param.addValue("feeRequest", feeRequest);
        return sqlRunner.execute(/* skip_tenant_check */
                """
                UPDATE cms_billing
                SET    status     = 'REQUESTED',
                       file_id    = :fileId,
                       result_msg = NULL,
                       _modified  = NOW(),
                       fee_request = fee_request + :feeRequest
                WHERE  id IN (:ids)
                  AND  status IN ('PENDING', 'ERROR')
                """, param);
    }

    // ─── 내부 헬퍼 ───────────────────────────────────────────────────────────

    /** billing_seq 채번: 해당 billing_ym의 다음 순번 반환 */
    private int getNextBillingSeqNo(String spjangcd, String billingYm) {
        var param = new org.springframework.jdbc.core.namedparam.MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);
        param.addValue("billingYm", billingYm);

        String sql = """
                SELECT COALESCE(MAX(CAST(SPLIT_PART(billing_seq, '-', 2) AS INTEGER)), 0) AS max_seq
                FROM cms_billing
                WHERE spjangcd = :spjangcd AND billing_ym = :billingYm
                """;
        Map<String, Object> row = sqlRunner.getRow(sql, param);
        return row != null ? ((Number) row.get("max_seq")).intValue() + 1 : 1;
    }

    /** 수납내역 조회 (페이징, 화면용) */
    public Map<String, Object> getBillingHistoryList(
            String startDate, String endDate, String billingType,
            String status, String memberName, int page, int size) {

        String spjangcd = TenantContext.get();
        var param = new org.springframework.jdbc.core.namedparam.MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);
        param.addValue("startDate", startDate);
        param.addValue("endDate",   endDate);

        String baseWhere =
                "  FROM cms_billing b" +
                        "  LEFT JOIN cms_bank_code bc ON bc.bank_code = b.bank_code" +
                        "  WHERE b.spjangcd = :spjangcd" +
                        "    AND b.deduct_date BETWEEN :startDate AND :endDate";

        String filters = "";
        String effectiveStatus = StringUtils.hasText(status) ? status : "SUCCESS,FAIL";
        if (effectiveStatus.contains(",")) {
            param.addValue("statusList", Arrays.asList(effectiveStatus.split(",")));
            filters += " AND b.status IN (:statusList)";
        } else {
            param.addValue("status", effectiveStatus);
            filters += " AND b.status = :status";
        }
        if (StringUtils.hasText(billingType)) {
            filters += " AND b.deduct_type = :billingType";
            param.addValue("billingType", billingType);
        }
        if (StringUtils.hasText(memberName)) {
            filters += " AND b.member_name LIKE '%' || :memberName || '%'";
            param.addValue("memberName", memberName);
        }

        Map<String, Object> aggRow = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT COUNT(*) AS cnt," +
                        "       COALESCE(SUM(b.billing_amount),0) AS total_amount," +
                        "       COALESCE(SUM(CASE WHEN b.status='SUCCESS' THEN b.fee_request+b.fee_success" +
                        "                         WHEN b.status='FAIL'    THEN b.fee_request ELSE 0 END),0) AS total_fee," +
                        "       COUNT(CASE WHEN b.status='SUCCESS' THEN 1 END) AS success_count," +
                        "       COUNT(CASE WHEN b.status='FAIL'    THEN 1 END) AS fail_count," +
                        "       COALESCE(SUM(CASE WHEN b.status='SUCCESS' THEN b.billing_amount ELSE 0 END),0) AS success_billing_amount," +
                        "       COALESCE(SUM(CASE WHEN b.status='FAIL'    THEN b.billing_amount ELSE 0 END),0) AS fail_amount," +
                        "       COALESCE(SUM(CASE WHEN b.status='SUCCESS'" +
                        "                         THEN b.billing_amount-(b.fee_request+b.fee_success) ELSE 0 END),0) AS success_deposit_amount" +
                        baseWhere + filters, param);
        long totalCount  = aggRow != null ? ((Number) aggRow.get("cnt")).longValue()          : 0L;
        long totalAmount = aggRow != null ? ((Number) aggRow.get("total_amount")).longValue() : 0L;
        long totalFee    = aggRow != null ? ((Number) aggRow.get("total_fee")).longValue()    : 0L;
        long successCount          = aggRow != null ? ((Number) aggRow.get("success_count")).longValue()          : 0L;
        long failCount             = aggRow != null ? ((Number) aggRow.get("fail_count")).longValue()             : 0L;
        long successBillingAmount  = aggRow != null ? ((Number) aggRow.get("success_billing_amount")).longValue() : 0L;
        long failAmount            = aggRow != null ? ((Number) aggRow.get("fail_amount")).longValue()            : 0L;
        long successDepositAmount  = aggRow != null ? ((Number) aggRow.get("success_deposit_amount")).longValue() : 0L;

        String dataSql =
                "SELECT b.id, b.billing_seq, b.deduct_type AS billing_type, b.member_name," +
                        "       b.bank_code, bc.bank_name, b.bank_account, b.billing_amount, b.deduct_date," +
                        "       b.status, b.result_code, b.result_msg, b.result_date," +
                        "       CASE WHEN EXISTS (SELECT 1 FROM cms_billing rb WHERE rb.spjangcd=b.spjangcd" +
                        "              AND rb.id > b.id AND rb.status NOT IN ('CANCEL','FAIL','ERROR')" +
                        "              AND ( (b.erp_mis_key IS NOT NULL AND rb.erp_mis_key = b.erp_mis_key)" +
                        "                 OR (b.erp_mis_key IS NULL AND rb.member_id=b.member_id" +
                        "                     AND rb.memo LIKE '%불능 / 재청구%' AND rb.deduct_date > b.deduct_date) )" +
                        "              ) THEN 'Y' ELSE 'N' END AS recharged_yn," +
                        "       CASE WHEN b.status='SUCCESS' THEN b.fee_request+b.fee_success" +
                        "            WHEN b.status='FAIL'    THEN b.fee_request ELSE 0 END AS fee_total" +
                        baseWhere + filters +
                        " ORDER BY b.deduct_date DESC, b.billing_seq LIMIT :pgSize OFFSET :pgOffset";

        param.addValue("pgSize",   size);
        param.addValue("pgOffset", (long) page * size);
        List<Map<String, Object>> rows = sqlRunner.getRows(dataSql, param);

        var result = new java.util.HashMap<String, Object>();
        result.put("data", rows);
        result.put("totalCount",  totalCount);
        result.put("totalAmount", totalAmount);
        result.put("totalFee",    totalFee);
        // ⭐ 전체(검색조건 전체) 기준 성공/실패 집계 — 상단 요약 카드용
        result.put("successCount",        successCount);
        result.put("failCount",           failCount);
        result.put("successAmount",       successDepositAmount);   // 성공 입금금액(수수료 차감)
        result.put("failAmount",          failAmount);             // 실패 청구금액
        result.put("successBillingAmount", successBillingAmount);  // 수납률 분자용(성공 청구금액)
        return result;
    }

    // 2️⃣ 재청구용 조회 (FAIL, ERROR 포함)
    public List<Map<String, Object>> getBillingHistoryForRecharge(
            String startDate, String endDate, String billingType) {

        return getBillingHistoryListInternal(
                startDate, endDate, billingType, "FAIL,ERROR", null,
                true, null  // ✅ rechargeFilter=true, 모든 상태 허용
        );
    }

    // 3️⃣ 운영용 조회 (모든 상태 포함)
    public List<Map<String, Object>> getBillingHistoryForAdmin(
            String startDate, String endDate, String billingType,
            String status, String memberName) {

        return getBillingHistoryListInternal(
                startDate, endDate, billingType, status, memberName,
                false, null  // ✅ 필터링 안 함
        );
    }

    // 4️⃣ 공통 로직
    private List<Map<String, Object>> getBillingHistoryListInternal(
            String startDate, String endDate, String billingType, String status,
            String memberName, boolean rechargeFilter, String defaultStatus) {

        String spjangcd = TenantContext.get();
        var param = new org.springframework.jdbc.core.namedparam.MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);
        param.addValue("startDate", startDate);
        param.addValue("endDate", endDate);

        String sql = """
        SELECT b.id, b.billing_seq, b.deduct_type AS billing_type,
               b.member_name, b.bank_code, bc.bank_name,
               b.bank_account, b.billing_amount, b.deduct_date,
               b.status, b.result_code, b.result_msg, b.result_date,
               CASE WHEN EXISTS (
                  SELECT 1 FROM cms_billing rb
                  WHERE rb.spjangcd = b.spjangcd
                    AND rb.id > b.id
                    AND rb.status NOT IN ('CANCEL', 'FAIL', 'ERROR')
                    AND ( (b.erp_mis_key IS NOT NULL AND rb.erp_mis_key = b.erp_mis_key)
                       OR (b.erp_mis_key IS NULL AND rb.member_id = b.member_id
                           AND rb.memo LIKE '%불능 / 재청구%' AND rb.deduct_date > b.deduct_date) )
              ) THEN 'Y' ELSE 'N' END AS recharged_yn,
              CASE
                 WHEN b.status = 'SUCCESS' THEN b.fee_request + b.fee_success
                 WHEN b.status = 'FAIL'    THEN b.fee_request
                 ELSE 0
               END AS fee_total
        FROM cms_billing b
        LEFT JOIN cms_bank_code bc ON bc.bank_code = b.bank_code
        WHERE b.spjangcd = :spjangcd
          AND b.deduct_date BETWEEN :startDate AND :endDate
        """;

        // ✅ 기본 상태값 적용
        String effectiveStatus = StringUtils.hasText(status) ? status : defaultStatus;

        if (StringUtils.hasText(effectiveStatus)) {
            if (effectiveStatus.contains(",")) {
                List<String> statusList = Arrays.asList(effectiveStatus.split(","));
                sql += " AND b.status IN (:statusList)";
                param.addValue("statusList", statusList);
            } else {
                sql += " AND b.status = :status";
                param.addValue("status", effectiveStatus);
            }
        }

        if (rechargeFilter) {
            sql += """
                  AND NOT EXISTS (
                      SELECT 1 FROM cms_billing rb
                      WHERE rb.spjangcd  = b.spjangcd
                        AND rb.id > b.id
                        AND rb.status NOT IN ('CANCEL', 'FAIL', 'ERROR')
                        AND ( (b.erp_mis_key IS NOT NULL AND rb.erp_mis_key = b.erp_mis_key)
                           OR (b.erp_mis_key IS NULL AND rb.member_id = b.member_id
                               AND rb.memo LIKE '%불능 / 재청구%' AND rb.deduct_date > b.deduct_date) )
                  )
                """;
        }

        if (StringUtils.hasText(billingType)) {
            sql += " AND b.deduct_type = :billingType";
            param.addValue("billingType", billingType);
        }

        if (StringUtils.hasText(memberName)) {
            sql += " AND b.member_name LIKE '%' || :memberName || '%'";
            param.addValue("memberName", memberName);
        }

        sql += " ORDER BY b.deduct_date DESC, b.billing_seq";
        return sqlRunner.getRows(sql, param);
    }

    // SFTP 실패 시
    public int updateStatusToError(List<Long> billingIds, String errorMsg) {
        if (billingIds == null || billingIds.isEmpty()) return 0;
        var param = new MapSqlParameterSource();
        param.addValue("ids", billingIds);
        param.addValue("errorMsg", errorMsg);
        return sqlRunner.execute(/* skip_tenant_check */
                """
                UPDATE cms_billing
                    SET status = 'ERROR', result_msg = :errorMsg, _modified = NOW()
                    WHERE id IN (:ids)
                      AND status IN ('PENDING', 'REQUESTED')
                """, param);
    }


    public List<Map<String, Object>> getSendableDates(String billingYm, String deductType) {
        String spjangcd = TenantContext.get();
        String todayStr = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        var param = new MapSqlParameterSource();
        param.addValue("spjangcd",   spjangcd);
        param.addValue("billingYm",  billingYm);
        param.addValue("deductType", deductType != null ? deductType : "EB");

        return sqlRunner.getRows("""
        SELECT b.deduct_date,
               b.send_date,
               COUNT(*)                    AS count,
               COALESCE(SUM(b.billing_amount), 0) AS total_amount
        FROM cms_billing b
        WHERE b.spjangcd    = :spjangcd
          AND b.billing_ym  = :billingYm
          AND b.deduct_type = :deductType
          AND b.status      = 'PENDING'
          AND b.send_date  IS NOT NULL
        GROUP BY b.deduct_date, b.send_date
        ORDER BY b.deduct_date
        """, param);
    }

    /**
     * ERP(TB_DA024) 미수금 후보를 조회해서 cms_member와 매칭한 목록을 리턴한다. (INSERT 안 함)
     * 모달에서 사용자가 선택할 목록을 만드는 용도.
     * 각 행 status: OK(생성 가능) / DUP(이미 청구됨) / NO_MEMBER(cms_member 없음) / NOT_AGREED(미인증)
     */
    public List<Map<String, Object>> previewErpBilling(String billingYm) {
        String spjangcd = TenantContext.get();

        Map<String, Object> erp = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT host, port, db_name, username, password, custcd FROM tb_xa012_erp WHERE spjangcd = :spjangcd",
                new MapSqlParameterSource("spjangcd", spjangcd));
        if (erp == null) throw new IllegalStateException("ERP 접속정보가 없습니다.");

        String custcd = str(erp.get("custcd"));
        String dbUrl = String.format("jdbc:sqlserver://%s:%s;databaseName=%s;encrypt=false",
                str(erp.get("host")), str(erp.get("port")), str(erp.get("db_name")));

        try { Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver"); }
        catch (ClassNotFoundException e) { throw new IllegalStateException("MSSQL 드라이버 없음"); }

        // 이미 가져온 erp_mis_key (중복 표시용)
        List<Map<String, Object>> existingKeys = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT erp_mis_key FROM cms_billing
                WHERE spjangcd = :spjangcd
                AND billing_ym = :billingYm
                AND erp_mis_key IS NOT NULL
                """,
                new MapSqlParameterSource("spjangcd", spjangcd)
                        .addValue("billingYm", billingYm));
        Set<String> existingMisKeys = existingKeys.stream()
                .map(r -> str(r.get("erp_mis_key")))
                .collect(Collectors.toSet());

        List<Map<String, Object>> list = new ArrayList<>();

        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(
                dbUrl, str(erp.get("username")), str(erp.get("password")))) {

            String sql = """
            SELECT
                D.cltcd,
                D.actcd,
                D.misdate,
                D.misnum,
                C.cltnm                           AS member_name,
                SUM(D.samt + ISNULL(D.addamt, 0)) AS billing_amount
            FROM TB_DA024 D WITH(NOLOCK)
            INNER JOIN TB_XCLIENT C WITH(NOLOCK)
                ON D.cltcd = C.cltcd
                AND C.custcd = ?
                AND C.accnum IS NOT NULL
                AND LTRIM(RTRIM(C.accnum)) != ''
                AND C.cmsrnum IS NOT NULL
                AND LTRIM(RTRIM(C.cmsrnum)) != ''
            WHERE D.custcd = ?
            AND LEFT(D.misdate, 6) = ?
            AND D.samt > 0
            GROUP BY D.cltcd, D.actcd, D.misdate, D.misnum, C.cltnm
            ORDER BY D.cltcd, D.misdate
            """;

            try (java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, custcd);
                ps.setString(2, custcd);
                ps.setString(3, billingYm);

                try (java.sql.ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String cltcd         = rs.getString("cltcd");
                        String actcd         = rs.getString("actcd");
                        String misdate       = rs.getString("misdate");
                        String misnum        = rs.getString("misnum");
                        String misKey        = misdate + misnum;
                        double billingAmount = rs.getDouble("billing_amount");
                        String memberName    = rs.getString("member_name");

                        if (!StringUtils.hasText(actcd)) actcd = cltcd;
                        if (billingAmount <= 0) continue;

                        Map<String, Object> member = sqlRunner.getRow(/* skip_tenant_check */
                                """
                                SELECT id, member_name, member_no, bank_code, bank_account,
                                       account_holder, deduct_day, agree_yn
                                FROM cms_member
                                WHERE spjangcd = :spjangcd AND cltcd = :actcd
                                """,
                                new MapSqlParameterSource("spjangcd", spjangcd)
                                        .addValue("actcd", actcd));

                        String rowStatus;
                        Object memberId = null;
                        String deductDay = "", deductDate = "";
                        String dispName = memberName;
                        String bankCode = "", bankAccount = "", accountHolder = "";

                        if (existingMisKeys.contains(misKey)) {
                            rowStatus = "DUP";
                        } else if (member == null) {
                            rowStatus = "NO_MEMBER";
                        } else if (!"Y".equals(str(member.get("agree_yn")))) {
                            rowStatus = "NOT_AGREED";
                            memberId  = member.get("id");
                            dispName  = str(member.get("member_name"));
                        } else {
                            rowStatus     = "OK";
                            memberId      = member.get("id");
                            dispName      = str(member.get("member_name"));
                            deductDay     = str(member.get("deduct_day"));
                            deductDate    = calcDeductDate(billingYm, deductDay);
                            bankCode      = str(member.get("bank_code"));
                            bankAccount   = str(member.get("bank_account"));
                            accountHolder = str(member.get("account_holder"));
                        }

                        Map<String, Object> row = new HashMap<>();
                        row.put("erp_mis_key",    misKey);
                        row.put("cltcd",          cltcd);
                        row.put("actcd",          actcd);
                        row.put("member_id",      memberId);
                        row.put("member_name",    dispName);
                        row.put("billing_amount", (long) billingAmount);
                        row.put("bank_code",      bankCode);
                        row.put("bank_account",   bankAccount);
                        row.put("account_holder", accountHolder);
                        row.put("deduct_day",     deductDay);
                        row.put("deduct_date",    deductDate);
                        row.put("row_status",     rowStatus);
                        list.add(row);
                    }
                }
            }
        } catch (Exception e) {
            log.error("[ERP청구-preview] 실패 spjangcd={}: {}", spjangcd, e.getMessage(), e);
            throw new IllegalStateException("ERP 미수금 조회 실패: " + e.getMessage());
        }

        return list;
    }

    /**
     * 모달에서 선택된 erp_mis_key 목록으로만 cms_billing 생성.
     * sendDate(출금신청일)는 사용자가 지정한 값을 그대로 사용. 출금일(deduct_date)은 cms_member 약정일 기준 계산.
     */
    @Transactional
    public Map<String, Object> createErpBilling(String billingYm, String sendDate,
                                                List<String> selectedKeys, String userId) {
        String spjangcd = TenantContext.get();
        int inserted = 0, skipped = 0;
        List<Map<String, String>> notFound = new ArrayList<>();

        if (selectedKeys == null || selectedKeys.isEmpty()) {
            throw new IllegalStateException("선택된 청구 건이 없습니다.");
        }
        if (!StringUtils.hasText(sendDate)) {
            throw new IllegalStateException("출금신청일을 지정하세요.");
        }
        sendDate = sendDate.replace("-", "");
        Set<String> wanted = new HashSet<>(selectedKeys);

        // preview로 후보를 다시 만들어 선택분만 신뢰성 있게 생성 (금액/회원 재검증)
        List<Map<String, Object>> candidates = previewErpBilling(billingYm);

        for (Map<String, Object> c : candidates) {
            String misKey = str(c.get("erp_mis_key"));
            if (!wanted.contains(misKey)) continue;

            String rowStatus = str(c.get("row_status"));
            String memberName = str(c.get("member_name"));

            if (!"OK".equals(rowStatus)) {
                // DUP / NO_MEMBER / NOT_AGREED 는 생성 안 함
                notFound.add(Map.of("cltcd", str(c.get("cltcd")),
                        "member_name", memberName + "(" + rowStatus + ")"));
                skipped++;
                continue;
            }

            String deductDate = str(c.get("deduct_date"));
            String deductDay  = str(c.get("deduct_day"));
            long   amount     = ((Number) c.get("billing_amount")).longValue();
            String billingSeq = generateBillingSeq(spjangcd, billingYm);

            MapSqlParameterSource param = new MapSqlParameterSource();
            param.addValue("spjangcd",      spjangcd);
            param.addValue("billingYm",     billingYm);
            param.addValue("memberId",      c.get("member_id"));
            param.addValue("memberName",    memberName);
            param.addValue("bankCode",      c.get("bank_code"));
            param.addValue("bankAccount",   c.get("bank_account"));
            param.addValue("accountHolder", c.get("account_holder"));
            param.addValue("deductDay",     deductDay);
            param.addValue("billingAmount", amount);
            param.addValue("deductDate",    deductDate);
            param.addValue("sendDate",      sendDate);        // 사용자 지정 출금신청일
            param.addValue("erpMisKey",     misKey);
            param.addValue("userId",        userId);
            param.addValue("billingSeq",    billingSeq);

            int rows = sqlRunner.execute(/* skip_tenant_check */
                    """
                    INSERT INTO cms_billing (
                        spjangcd, billing_ym, billing_seq,
                        member_id, member_name,
                        bank_code, bank_account, account_holder,
                        deduct_day, billing_amount, deduct_date, send_date,
                        deduct_type, status, erp_mis_key,
                        _creater_id, _created, _modifier_id, _modified
                    ) VALUES (
                        :spjangcd, :billingYm, :billingSeq,
                        :memberId, :memberName,
                        :bankCode, :bankAccount, :accountHolder,
                        :deductDay, :billingAmount, :deductDate, :sendDate,
                        'EB', 'PENDING', :erpMisKey,
                        :userId, NOW(), :userId, NOW()
                    )
                    """, param);

            if (rows > 0) inserted++;
            else { skipped++; }
        }

        log.info("[ERP청구-create] 완료 spjangcd={} 신규={} 스킵={} 미등록={}",
                spjangcd, inserted, skipped, notFound.size());

        return Map.of("inserted", inserted, "skipped", skipped, "notFound", notFound);
    }

    /** ERP TB_DA023 → cms_billing 청구 가져오기 */
    /** ERP TB_DA024 → cms_billing 청구 가져오기 (구버전: 전체 자동 생성) */
    public Map<String, Object> importFromErp(String billingYm, String userId) {
        String spjangcd = TenantContext.get();
        int inserted = 0, skipped = 0;
        List<Map<String, String>> notFound = new ArrayList<>();

        // ERP 접속정보 조회
        Map<String, Object> erp = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT host, port, db_name, username, password, custcd FROM tb_xa012_erp WHERE spjangcd = :spjangcd",
                new MapSqlParameterSource("spjangcd", spjangcd));
        if (erp == null) throw new IllegalStateException("ERP 접속정보가 없습니다.");

        String custcd = str(erp.get("custcd"));
        String dbUrl = String.format("jdbc:sqlserver://%s:%s;databaseName=%s;encrypt=false",
                str(erp.get("host")), str(erp.get("port")), str(erp.get("db_name")));

        try { Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver"); }
        catch (ClassNotFoundException e) { throw new IllegalStateException("MSSQL 드라이버 없음"); }

        // 이미 가져온 erp_mis_key 목록 (중복 방지)
        List<Map<String, Object>> existingKeys = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT erp_mis_key FROM cms_billing
                WHERE spjangcd = :spjangcd
                AND billing_ym = :billingYm
                AND erp_mis_key IS NOT NULL
                """,
                new MapSqlParameterSource("spjangcd", spjangcd)
                        .addValue("billingYm", billingYm));

        Set<String> existingMisKeys = existingKeys.stream()
                .map(r -> str(r.get("erp_mis_key")))
                .collect(Collectors.toSet());

        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(
                dbUrl, str(erp.get("username")), str(erp.get("password")))) {

            // TB_DA024에 actcd 컬럼이 있으므로 바로 사용
            String sql = """
            SELECT
                D.cltcd,
                D.actcd,
                D.misdate,
                D.misnum,
                C.cltnm                           AS member_name,
                SUM(D.samt + ISNULL(D.addamt, 0)) AS billing_amount
            FROM TB_DA024 D WITH(NOLOCK)
            INNER JOIN TB_XCLIENT C WITH(NOLOCK)
                ON D.cltcd = C.cltcd
                AND C.custcd = ?
                AND C.accnum IS NOT NULL
                AND LTRIM(RTRIM(C.accnum)) != ''
                AND C.cmsrnum IS NOT NULL
                AND LTRIM(RTRIM(C.cmsrnum)) != ''
            WHERE D.custcd = ?
            AND LEFT(D.misdate, 6) = ?
            AND D.samt > 0
            GROUP BY D.cltcd, D.actcd, D.misdate, D.misnum, C.cltnm
            ORDER BY D.cltcd, D.misdate
            """;

            try (java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, custcd);
                ps.setString(2, custcd);
                ps.setString(3, billingYm);

                try (java.sql.ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String cltcd         = rs.getString("cltcd");
                        String actcd         = rs.getString("actcd");
                        String misdate       = rs.getString("misdate");
                        String misnum        = rs.getString("misnum");
                        String misKey        = misdate + misnum;
                        double billingAmount = rs.getDouble("billing_amount");
                        String memberName    = rs.getString("member_name");

                        // actcd 없으면 cltcd 사용
                        if (!StringUtils.hasText(actcd)) actcd = cltcd;

                        if (billingAmount <= 0) { skipped++; continue; }

                        // 중복 체크
                        if (existingMisKeys.contains(misKey)) {
                            log.info("[ERP청구] 중복 스킵 cltcd={} actcd={} misKey={}", cltcd, actcd, misKey);
                            skipped++;
                            continue;
                        }

                        // cms_member 조회 (actcd 기준)
                        Map<String, Object> member = sqlRunner.getRow(/* skip_tenant_check */
                                """
                                SELECT id, member_name, member_no, bank_code, bank_account,
                                       account_holder, deduct_day, agree_yn
                                FROM cms_member
                                WHERE spjangcd = :spjangcd AND cltcd = :actcd
                                """,
                                new MapSqlParameterSource("spjangcd", spjangcd)
                                        .addValue("actcd", actcd));

                        if (member == null) {
                            log.warn("[ERP청구] cms_member 없음 cltcd={} actcd={} memberName={}", cltcd, actcd, memberName);
                            notFound.add(Map.of(
                                    "cltcd",       cltcd,
                                    "member_name", memberName
                            ));
                            skipped++;
                            continue;
                        }

                        String agreeYn = str(member.get("agree_yn"));
                        if (!"Y".equals(agreeYn)) {
                            log.warn("[ERP청구] 미인증 납부자 스킵 cltcd={} actcd={} memberName={}", cltcd, actcd, memberName);
                            notFound.add(Map.of("cltcd", cltcd, "member_name", memberName + "(미인증)"));
                            skipped++;
                            continue;
                        }

                        // 출금일 계산
                        String deductDay  = str(member.get("deduct_day"));
                        String deductDate = calcDeductDate(billingYm, deductDay);
                        String sendDate   = calcSendDate(deductDate, "EB");

                        String billingSeq = generateBillingSeq(spjangcd, billingYm);

                        // cms_billing INSERT
                        MapSqlParameterSource param = new MapSqlParameterSource();
                        param.addValue("spjangcd",      spjangcd);
                        param.addValue("billingYm",     billingYm);
                        param.addValue("memberId",      member.get("id"));
                        param.addValue("memberName",    member.get("member_name"));
                        param.addValue("bankCode",      member.get("bank_code"));
                        param.addValue("bankAccount",   member.get("bank_account"));
                        param.addValue("accountHolder", member.get("account_holder"));
                        param.addValue("deductDay",     deductDay);
                        param.addValue("billingAmount", (long) billingAmount);
                        param.addValue("deductDate",    deductDate);
                        param.addValue("sendDate",      sendDate);
                        param.addValue("erpMisKey",     misKey);
                        param.addValue("userId",        userId);
                        param.addValue("billingSeq",    billingSeq);

                        int rows = sqlRunner.execute(/* skip_tenant_check */
                                """
                                INSERT INTO cms_billing (
                                    spjangcd, billing_ym, billing_seq,
                                    member_id, member_name,
                                    bank_code, bank_account, account_holder,
                                    deduct_day, billing_amount, deduct_date, send_date,
                                    deduct_type, status, erp_mis_key,
                                    _creater_id, _created, _modifier_id, _modified
                                ) VALUES (
                                    :spjangcd, :billingYm, :billingSeq,
                                    :memberId, :memberName,
                                    :bankCode, :bankAccount, :accountHolder,
                                    :deductDay, :billingAmount, :deductDate, :sendDate,
                                    'EB', 'PENDING', :erpMisKey,
                                    :userId, NOW(), :userId, NOW()
                                )
                                """, param);

                        log.info("[ERP청구] INSERT rowEffected={} cltcd={} actcd={} misKey={}", rows, cltcd, actcd, misKey);

                        if (rows > 0) {
                            existingMisKeys.add(misKey);
                            inserted++;
                        } else {
                            log.error("[ERP청구] INSERT 실패 (0 rows) cltcd={} actcd={} misKey={}", cltcd, actcd, misKey);
                            skipped++;
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("[ERP청구] 실패 spjangcd={}: {}", spjangcd, e.getMessage(), e);
            throw new IllegalStateException("ERP 청구 가져오기 실패: " + e.getMessage());
        }

        log.info("[ERP청구] 완료 spjangcd={} 신규={} 스킵={} 미등록납부자={}",
                spjangcd, inserted, skipped, notFound.size());

        return Map.of(
                "inserted", inserted,
                "skipped",  skipped,
                "notFound", notFound
        );
    }

    private String calcDeductDate(String yyyymm, String deductDay) {
        if (!StringUtils.hasText(deductDay)) deductDay = "25";
        int year  = Integer.parseInt(yyyymm.substring(0, 4));
        int month = Integer.parseInt(yyyymm.substring(4, 6));
        LocalDate base = LocalDate.of(year, month, 1);

        if ("99".equals(deductDay)) {
            LocalDate lastDay = base.withDayOfMonth(base.lengthOfMonth());
            return cmsHolidayService.getPrevBusinessDay(
                    lastDay.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
        }
        int day = Integer.parseInt(deductDay);
        if (day > base.lengthOfMonth()) day = base.lengthOfMonth();
        return cmsHolidayService.getNextBusinessDay(
                LocalDate.of(year, month, day).format(DateTimeFormatter.ofPattern("yyyyMMdd")));
    }

    private String str(Object v) { return v != null ? v.toString() : ""; }

    /**
     * 수동 청구 생성 전 약정일별 청구 가능 건수 조회
     * - 오늘 기준 청구 가능한 약정일만 반환
     * - 이미 해당 월에 청구된 건은 제외
     */
    public List<Map<String, Object>> getDeductDaySummary(String billingYm, String deductType) {
        String spjangcd = TenantContext.get();
        YearMonth ym = YearMonth.parse(billingYm, DateTimeFormatter.ofPattern("yyyyMM"));
        String firstDay = ym.atDay(1).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String lastDay  = ym.atEndOfMonth().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String monthStr = String.valueOf(ym.getMonthValue());
        String todayStr = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String effectiveDeductType = deductType != null ? deductType : "EB";
        int nowHour = java.time.LocalTime.now().getHour();

        // 청구 가능한 납부자 조회 (이미 청구된 건 제외)
        List<Map<String, Object>> members = sqlRunner.getRows("""
        SELECT m.id, m.deduct_day, m.deduct_amount, m.pause_start_date, m.pause_end_date
        FROM cms_member m
        WHERE m.spjangcd  = :spjangcd
          AND m.status    = 'ACTIVE'
          AND m.agree_yn  = 'Y'
          AND m.start_date <= :lastDay
          AND m.end_date   >= :firstDay
          AND (
              m.cycle_type = 'REGULAR'
              OR (m.cycle_type = 'IRREGULAR' AND :monthStr = ANY(STRING_TO_ARRAY(m.cycle_months, ',')))
          )
          AND NOT EXISTS (
              SELECT 1 FROM cms_billing b
              WHERE b.member_id   = m.id
                AND b.billing_ym  = :billingYm
                AND b.spjangcd    = :spjangcd
                AND b.deduct_type = :deductType
          )
        ORDER BY m.deduct_day
        """,
                new MapSqlParameterSource()
                        .addValue("spjangcd",   spjangcd)
                        .addValue("billingYm",  billingYm)
                        .addValue("firstDay",   firstDay)
                        .addValue("lastDay",    lastDay)
                        .addValue("monthStr",   monthStr)
                        .addValue("deductType", effectiveDeductType));

        // 약정일별로 그룹핑하면서 청구 가능 여부 체크
        Map<String, Integer> dayCntMap    = new LinkedHashMap<>();
        Map<String, Boolean> dayAvailMap  = new LinkedHashMap<>();
        Map<String, String>  dayReasonMap = new LinkedHashMap<>();

        for (Map<String, Object> m : members) {
            String deductDay = (String) m.get("deduct_day");
            if (deductDay == null || deductDay.isEmpty()) continue;

            // 중지 기간 체크
            if (isPausedInBillingMonth(m, ym)) continue;

            // 출금일 계산
            String deductDate = "99".equals(deductDay) ? lastDay : billingYm + deductDay;
            deductDate = cmsHolidayService.getNextBusinessDay(deductDate);

            // 청구 가능 여부 판단
            boolean available = true;
            String reason = "";

            if (deductDate.compareTo(todayStr) < 0) {
                available = false;
                reason = "출금일 경과";
            } else if ("EB".equals(effectiveDeductType)) {
                String deadlineDay = cmsHolidayService.getPrevBusinessDay(
                        LocalDate.parse(deductDate, DateTimeFormatter.ofPattern("yyyyMMdd"))
                                .minusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMdd")));
                if (deadlineDay.compareTo(todayStr) < 0) {
                    available = false;
                    reason = "신청마감 경과";
                } else if (deadlineDay.equals(todayStr) && nowHour >= 17) {
                    available = false;
                    reason = "오늘 15시 마감 초과";
                }
            } else if ("EC".equals(effectiveDeductType)) {
                if (deductDate.equals(todayStr) && nowHour >= 11) {
                    available = false;
                    reason = "오늘 11시 마감 초과";
                }
            }

            // 약정일별 집계
            String displayDay = "99".equals(deductDay) ? "말일" : Integer.parseInt(deductDay) + "일";
            dayCntMap.merge(displayDay, 1, Integer::sum);
            // available이 한 번이라도 true면 가능
            dayAvailMap.merge(displayDay, available, (a, b) -> a || b);
            if (!available && !dayReasonMap.containsKey(displayDay)) {
                dayReasonMap.put(displayDay, reason);
            }
        }

        List<Map<String, Object>> result = new ArrayList<>();
        for (String day : dayCntMap.keySet()) {
            Map<String, Object> row = new HashMap<>();
            row.put("deduct_day",  day);
            row.put("count",       dayCntMap.get(day));
            row.put("available",   dayAvailMap.getOrDefault(day, false));
            row.put("reason",      dayReasonMap.getOrDefault(day, ""));
            result.add(row);
        }
        return result;
    }

    /**
     * 수동 청구 생성 - 선택한 약정일들로 청구 생성
     * send_date = 오늘 (수동 즉시 생성이므로)
     */
    @Transactional
    public Map<String, Object> generateBillingManual(String billingYm, List<String> deductDays, String deductType, String userId) {
        String spjangcd = TenantContext.get();
        YearMonth ym = YearMonth.parse(billingYm, DateTimeFormatter.ofPattern("yyyyMM"));
        String firstDay = ym.atDay(1).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String lastDay  = ym.atEndOfMonth().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String monthStr = String.valueOf(ym.getMonthValue());
        String todayStr = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String effectiveDeductType = deductType != null ? deductType : "EB";
        int nowHour = java.time.LocalTime.now().getHour();

        // 선택한 약정일 목록 (말일 → 99 변환)
        List<String> normalizedDays = deductDays.stream()
                .map(d -> "말일".equals(d) ? "99" : String.format("%02d", Integer.parseInt(d.replace("일", ""))))
                .collect(java.util.stream.Collectors.toList());

        // 대상 납부자 조회
        List<Map<String, Object>> members = sqlRunner.getRows("""
        SELECT m.id, m.member_name, m.bank_code, m.bank_account, m.account_holder,
               m.deduct_amount, m.deduct_day, m.pause_start_date, m.pause_end_date
        FROM cms_member m
        WHERE m.spjangcd  = :spjangcd
          AND m.status    = 'ACTIVE'
          AND m.agree_yn  = 'Y'
          AND m.start_date <= :lastDay
          AND m.end_date   >= :firstDay
          AND m.deduct_day = ANY(:deductDays::TEXT[])
          AND (
              m.cycle_type = 'REGULAR'
              OR (m.cycle_type = 'IRREGULAR' AND :monthStr = ANY(STRING_TO_ARRAY(m.cycle_months, ',')))
          )
          AND NOT EXISTS (
              SELECT 1 FROM cms_billing b
              WHERE b.member_id   = m.id
                AND b.billing_ym  = :billingYm
                AND b.spjangcd    = :spjangcd
                AND b.deduct_type = :deductType
          )
        ORDER BY m.deduct_day, m.id
        """,
                new MapSqlParameterSource()
                        .addValue("spjangcd",   spjangcd)
                        .addValue("billingYm",  billingYm)
                        .addValue("firstDay",   firstDay)
                        .addValue("lastDay",    lastDay)
                        .addValue("monthStr",   monthStr)
                        .addValue("deductDays", normalizedDays.toArray(new String[0]))
                        .addValue("deductType", effectiveDeductType));

        int count = 0, skippedCount = 0;
        int nextSeq = getNextBillingSeqNo(spjangcd, billingYm);

        for (Map<String, Object> m : members) {
            String deductDay  = (String) m.get("deduct_day");
            if (deductDay == null || deductDay.isEmpty()) { skippedCount++; continue; }

            // 중지 기간 체크
            if (isPausedInBillingMonth(m, ym)) { skippedCount++; continue; }

            String deductDate = "99".equals(deductDay) ? lastDay : billingYm + deductDay;
            deductDate = cmsHolidayService.getNextBusinessDay(deductDate);

            // 마감 체크
            if (deductDate.compareTo(todayStr) < 0) { skippedCount++; continue; }
            if ("EB".equals(effectiveDeductType)) {
                String deadlineDay = cmsHolidayService.getPrevBusinessDay(
                        LocalDate.parse(deductDate, DateTimeFormatter.ofPattern("yyyyMMdd"))
                                .minusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMdd")));
                if (deadlineDay.compareTo(todayStr) < 0) { skippedCount++; continue; }
                if (deadlineDay.equals(todayStr) && nowHour >= 17) { skippedCount++; continue; }
            }
            if ("EC".equals(effectiveDeductType) && deductDate.equals(todayStr) && nowHour >= 11) {
                skippedCount++; continue;
            }

            String billingSeq = billingYm + "-" + String.format("%04d", nextSeq++);

            // send_date = 오늘 (수동 생성)
            var ip = new MapSqlParameterSource();
            ip.addValue("spjangcd",      spjangcd);
            ip.addValue("billingYm",     billingYm);
            ip.addValue("billingSeq",    billingSeq);
            ip.addValue("memberId",      ((Number) m.get("id")).longValue());
            ip.addValue("memberName",    m.get("member_name"));
            ip.addValue("bankCode",      m.get("bank_code"));
            ip.addValue("bankAccount",   m.get("bank_account"));
            ip.addValue("accountHolder", m.get("account_holder"));
            ip.addValue("billingAmount", m.get("deduct_amount"));
            ip.addValue("deductDay",     deductDay);
            ip.addValue("deductDate",    deductDate);
            ip.addValue("sendDate",      todayStr);   // 수동 생성 → send_date = 오늘
            ip.addValue("deductType",    effectiveDeductType);
            ip.addValue("userId",        userId);

            sqlRunner.execute("""
            INSERT INTO cms_billing (
                spjangcd, billing_ym, billing_seq,
                member_id, member_name, bank_code, bank_account, account_holder,
                billing_amount, deduct_day, deduct_date, send_date,
                deduct_type, status, _creater_id, _created, _modifier_id, _modified
            ) VALUES (
                :spjangcd, :billingYm, :billingSeq,
                :memberId, :memberName, :bankCode, :bankAccount, :accountHolder,
                :billingAmount, :deductDay, :deductDate, :sendDate,
                :deductType, 'PENDING', :userId, NOW(), :userId, NOW()
            )
            """, ip);
            count++;
        }

        Map<String, Object> result = new HashMap<>();
        result.put("count",        count);
        result.put("skippedCount", skippedCount);
        return result;
    }

    public void updateSendDate(List<Long> billingIds, String actualSendDate) {
        if (billingIds == null || billingIds.isEmpty()) return;
        sqlRunner.execute(/* skip_tenant_check */
                """
                UPDATE cms_billing
                SET send_date    = :sendDate,
                    _modified    = NOW()
                WHERE id = ANY(:ids::BIGINT[])
                  AND status IN ('PENDING', 'REQUESTED')
                """,
                new MapSqlParameterSource()
                        .addValue("sendDate", actualSendDate)
                        .addValue("ids", billingIds.toArray(new Long[0])));
    }

    public int changeDeductDate(String ids, String deductDate) {
        String spjangcd = TenantContext.get();
        List<Long> idList = Arrays.stream(ids.split(","))
                .map(String::trim).map(Long::parseLong).collect(Collectors.toList());

        String sendDate = cmsHolidayService.getPrevBusinessDay(
                LocalDate.parse(deductDate, DateTimeFormatter.ofPattern("yyyyMMdd"))
                        .minusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMdd")));

        return sqlRunner.execute(/* skip_tenant_check */
                """
                UPDATE cms_billing
                SET deduct_date = :deductDate,
                    send_date   = :sendDate,
                    _modified   = NOW()
                WHERE id = ANY(:ids::BIGINT[])
                  AND spjangcd = :spjangcd
                  AND status = 'PENDING'
                """,
                new MapSqlParameterSource()
                        .addValue("ids", idList.toArray(new Long[0]))
                        .addValue("deductDate", deductDate)
                        .addValue("sendDate", sendDate)
                        .addValue("spjangcd", spjangcd));
    }


}