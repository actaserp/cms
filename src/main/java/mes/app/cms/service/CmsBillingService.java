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
                        "       m.member_no, m.id_number, m.biz_no, m.resident_no," +
                        "       b.bank_code, bc.bank_name, b.bank_account," +
                        "       b.account_holder, b.billing_amount, b.deduct_day, b.deduct_date," +
                        "       b.send_date, b.status, b.result_code, b.result_msg, b.result_date," +
                        "       b.memo, b.print_suffix, b._created, b._modified," +
                        "       CASE WHEN EXISTS (" +
                        "           SELECT 1 FROM cms_billing rb" +
                        "           WHERE rb.spjangcd = b.spjangcd AND rb.id > b.id" +
                        "             AND rb.status NOT IN ('CANCEL', 'FAIL', 'ERROR')" +
                        "             AND ( (b.erp_mis_key IS NOT NULL AND rb.erp_mis_key = b.erp_mis_key)" +
                        "                OR (b.erp_mis_key IS NULL AND rb.member_id = b.member_id" +
                        "                    AND rb.memo LIKE '%불능 / 재청구%' AND rb.deduct_date > b.deduct_date) )" +
                        "       ) THEN 'Y' ELSE 'N' END AS recharged_yn" +
                        baseWhere + filters +
                        // 같은 출금일자끼리는 납부자명 가나다순. 한글 완성형은 유니코드 코드포인트
                        // 순서가 곧 가나다순이라 별도 COLLATE 없이 정렬된다. 동명이인은 청구번호로 확정.
                        " ORDER BY b.deduct_date, b.member_name, b.billing_seq" +
                        " LIMIT :pgSize OFFSET :pgOffset";

        param.addValue("pgSize",   size);
        param.addValue("pgOffset", (long) page * size);
        List<Map<String, Object>> rows = sqlRunner.getRows(dataSql, param);

        var result = new java.util.HashMap<String, Object>();
        result.put("data", rows);
        result.put("totalCount",  totalCount);
        result.put("totalAmount", totalAmount);
        // 통장기재내용 앞부분(결제원 신고 문구). 화면에서 남은 바이트를 계산하는 데 쓴다.
        Map<String, Object> cms = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT cms_description FROM tb_xa012_cms WHERE spjangcd = :spjangcd LIMIT 1",
                new MapSqlParameterSource("spjangcd", spjangcd));
        result.put("cmsDescription", cms != null ? str(cms.get("cms_description")) : "");
        return result;
    }

    /**
     * 검색조건에 해당하는 '전체' 청구 id 목록.
     * 그리드 전체선택은 화면에 로드된 행(페이징 10건 / 무한스크롤 누적분)만 잡히므로,
     * 조건에 맞는 대상 전체를 잡으려면 서버에서 id 를 받아와야 한다.
     * 액션(재전송·출금일 변경·통장기재)은 PENDING 만 대상이라 여기서도 같은 기준으로 거른다.
     */
    public List<Long> getBillingIds(String billingYm, String sendDateFrom, String sendDateTo,
                                    String memberName, String status, String deductType) {
        String spjangcd = TenantContext.get();
        var param = new MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);
        param.addValue("deductType", deductType != null ? deductType : "EB");

        String sql = "SELECT b.id FROM cms_billing b"
                + " WHERE b.spjangcd = :spjangcd AND b.deduct_type = :deductType"
                + " AND b.status = 'PENDING'";

        if (StringUtils.hasText(sendDateFrom) && StringUtils.hasText(sendDateTo)) {
            sql += " AND b.send_date BETWEEN :sendDateFrom AND :sendDateTo";
            param.addValue("sendDateFrom", sendDateFrom);
            param.addValue("sendDateTo",   sendDateTo);
        } else if (StringUtils.hasText(sendDateFrom)) {
            sql += " AND b.send_date >= :sendDateFrom";
            param.addValue("sendDateFrom", sendDateFrom);
        } else if (StringUtils.hasText(sendDateTo)) {
            sql += " AND b.send_date <= :sendDateTo";
            param.addValue("sendDateTo", sendDateTo);
        }
        if (StringUtils.hasText(memberName)) {
            sql += " AND b.member_name LIKE '%' || :memberName || '%'";
            param.addValue("memberName", memberName);
        }
        if (StringUtils.hasText(billingYm)) {
            sql += " AND b.billing_ym = :billingYm";
            param.addValue("billingYm", billingYm);
        }
        sql += " ORDER BY b.id";

        return sqlRunner.getRows(/* skip_tenant_check */ sql, param).stream()
                .map(r -> ((Number) r.get("id")).longValue())
                .collect(Collectors.toList());
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

    // ─────────────────────────────────────────────────────────────
    // 엑셀 대량 업로드 (upsert)
    // ─────────────────────────────────────────────────────────────

    /** 상태 라벨(한글) → 코드 매핑 (엑셀 표시값도 허용) */
    private static final Map<String, String> STATUS_LABEL_TO_CODE = Map.of(
            "대기", "PENDING", "출금요청", "REQUESTED", "성공", "SUCCESS",
            "실패", "FAIL", "취소", "CANCEL", "오류", "ERROR");

    private static String rowStr(Map<String, Object> row, String key) {
        Object v = row.get(key);
        return v == null ? null : v.toString().trim();
    }

    /** 금액 문자열 정규화: "1,000", "1000원" → "1000" */
    private static Long parseAmount(String s) {
        if (s == null || s.isBlank()) return null;
        String digits = s.replaceAll("[^0-9-]", "");
        if (digits.isBlank() || "-".equals(digits)) return null;
        try { return Long.parseLong(digits); } catch (NumberFormatException e) { return null; }
    }

    /** 날짜 정규화: "2025-01-15", "20250115" → "20250115" (8자리만 유효) */
    private static String parseDate8(String s) {
        if (s == null) return null;
        String digits = s.replaceAll("[^0-9]", "");
        return digits.length() == 8 ? digits : null;
    }

    /** 약정일 정규화: "말일" → "99", "10일" → "10" */
    private static String parseDeductDay(String s) {
        if (s == null || s.isBlank()) return null;
        String t = s.trim();
        if (t.contains("말")) return "99";
        String digits = t.replaceAll("[^0-9]", "");
        return digits.isBlank() ? null : digits;
    }

    /** 상태 정규화: 코드 그대로거나 한글 라벨이면 코드로 변환 */
    private static String parseStatus(String s) {
        if (s == null || s.isBlank()) return null;
        String t = s.trim();
        if (STATUS_LABEL_TO_CODE.containsValue(t.toUpperCase())) return t.toUpperCase();
        return STATUS_LABEL_TO_CODE.get(t);
    }

    /**
     * 엑셀 행 배열을 upsert 처리한다.
     * @param rows 화면에서 파싱한 행 목록. 각 행 키:
     *             billing_seq, member_name, member_no, bank_name, bank_account,
     *             deduct_day, deduct_date, billing_amount, status, memo
     * @param defaultBillingYm 출금일자가 없을 때 사용할 청구년월(yyyyMM). 선택.
     * @param deductType 출금유형(기본 EB)
     * @return inserted/updated/skipped/failed 카운트와 상세 로그(details)
     */
    @Transactional
    public Map<String, Object> bulkUpsertBilling(List<Map<String, Object>> rows,
                                                 String defaultBillingYm,
                                                 String deductType,
                                                 String userId) {
        String spjangcd = TenantContext.get();
        String effDeductType = StringUtils.hasText(deductType) ? deductType : "EB";
        String defYm = defaultBillingYm != null ? defaultBillingYm.replace("-", "") : null;

        // ── 1단계: 전체 검증 & 실행 계획 수립 (DB 읽기만, 쓰기 없음) ──────────
        // Postgres는 트랜잭션 중 한 문장이 실패하면 이후 문장이 모두 중단되므로,
        // 먼저 모든 행을 검증해 실패가 하나라도 있으면 아무것도 쓰지 않고 반환한다(원자적).
        List<Map<String, Object>> details = new ArrayList<>();
        List<Runnable> writes = new ArrayList<>();
        int[] cnt = {0, 0, 0, 0}; // inserted, updated, skipped, failed

        int rowNo = 0;
        for (Map<String, Object> row : rows) {
            rowNo++;
            final int rn = rowNo;
            String billingSeq   = rowStr(row, "billing_seq");
            String memberName   = rowStr(row, "member_name");
            String memberNo     = rowStr(row, "member_no");
            String memo         = rowStr(row, "memo");
            Long   billingAmount = parseAmount(rowStr(row, "billing_amount"));
            String deductDate   = parseDate8(rowStr(row, "deduct_date"));
            // 계좌/은행/예금주/약정일/상태는 엑셀로 수정하지 않는다.
            // (계좌는 회원의 검증된 계좌 = EB13 실명확인/출금동의 기준이어야 하므로 잠금)

            if (StringUtils.hasText(billingSeq)) {
                // ── 수정: 청구번호로 기존 건을 찾아 금액/출금일자/메모만 반영 ──
                Map<String, Object> existing = sqlRunner.getRow(
                        "SELECT id, status, deduct_date, send_date, deduct_type " +
                                "FROM cms_billing WHERE spjangcd = :spjangcd AND billing_seq = :billingSeq",
                        new MapSqlParameterSource()
                                .addValue("spjangcd", spjangcd)
                                .addValue("billingSeq", billingSeq));

                if (existing == null) {
                    cnt[3]++;
                    details.add(detail(rn, billingSeq, "FAILED", "청구번호가 존재하지 않습니다."));
                    continue;
                }
                String curStatus = existing.get("status") != null ? existing.get("status").toString() : "";
                if (!"PENDING".equals(curStatus)) {
                    cnt[2]++;
                    details.add(detail(rn, billingSeq, "SKIPPED",
                            "대기(PENDING) 상태만 수정 가능(현재: " + curStatus + ")"));
                    continue;
                }

                Long id = ((Number) existing.get("id")).longValue();
                String curDeductType = existing.get("deduct_type") != null
                        ? existing.get("deduct_type").toString() : effDeductType;
                String curDeductDate = existing.get("deduct_date") != null ? existing.get("deduct_date").toString() : null;
                String newDeductDate = StringUtils.hasText(deductDate) ? deductDate : curDeductDate;
                String sendDate;
                if (StringUtils.hasText(newDeductDate) && !newDeductDate.equals(curDeductDate)) {
                    sendDate = calcSendDate(newDeductDate, curDeductType);
                } else {
                    sendDate = existing.get("send_date") != null ? existing.get("send_date").toString() : null;
                }
                final String fSendDate = sendDate, fNewDeductDate = newDeductDate;
                final Long fAmountU = billingAmount;
                final String fMemoU = memo;

                cnt[1]++;
                details.add(detail(rn, billingSeq, "UPDATED", "수정 예정 (금액/출금일자/메모)"));
                writes.add(() -> {
                    // COALESCE: 비어있는 셀은 기존값 유지. 계좌·납부자 정보는 건드리지 않음.
                    sqlRunner.execute("""
                            UPDATE cms_billing SET
                                billing_amount = COALESCE(:billingAmount, billing_amount),
                                deduct_date    = COALESCE(:deductDate, deduct_date),
                                send_date      = :sendDate,
                                memo           = COALESCE(:memo, memo),
                                _modifier_id   = :userId,
                                _modified      = NOW()
                            WHERE id = :id AND spjangcd = :spjangcd
                            """, new MapSqlParameterSource()
                            .addValue("id", id).addValue("spjangcd", spjangcd)
                            .addValue("billingAmount", fAmountU)
                            .addValue("deductDate", fNewDeductDate).addValue("sendDate", fSendDate)
                            .addValue("memo", fMemoU).addValue("userId", userId));
                });

            } else {
                // ── 신규: 납부자명(또는 납부자번호)으로 회원 매칭. 계좌는 회원에서 자동 ──
                if (!StringUtils.hasText(memberName) && !StringUtils.hasText(memberNo)) {
                    cnt[3]++;
                    details.add(detail(rn, null, "FAILED", "신규 등록에는 납부자명이 필요합니다."));
                    continue;
                }
                if (billingAmount == null) {
                    cnt[3]++;
                    details.add(detail(rn, null, "FAILED", "청구금액이 올바르지 않습니다."));
                    continue;
                }
                if (!StringUtils.hasText(deductDate)) {
                    cnt[3]++;
                    details.add(detail(rn, null, "FAILED", "출금일자가 필요합니다."));
                    continue;
                }

                // 납부자번호가 있으면 정확 매칭, 없으면 납부자명으로 매칭(동명이인 모두 대상)
                List<Map<String, Object>> members;
                if (StringUtils.hasText(memberNo)) {
                    members = sqlRunner.getRows(
                            "SELECT id, member_name, member_no, bank_code, bank_account, account_holder, deduct_day, agree_yn " +
                                    "FROM cms_member WHERE spjangcd = :spjangcd AND member_no = :memberNo",
                            new MapSqlParameterSource().addValue("spjangcd", spjangcd).addValue("memberNo", memberNo));
                } else {
                    members = sqlRunner.getRows(
                            "SELECT id, member_name, member_no, bank_code, bank_account, account_holder, deduct_day, agree_yn " +
                                    "FROM cms_member WHERE spjangcd = :spjangcd AND member_name = :memberName",
                            new MapSqlParameterSource().addValue("spjangcd", spjangcd).addValue("memberName", memberName));
                }

                if (members == null || members.isEmpty()) {
                    cnt[3]++;
                    details.add(detail(rn, null, "FAILED",
                            "일치하는 회원이 없습니다: " + (StringUtils.hasText(memberNo) ? memberNo : memberName)));
                    continue;
                }

                // 출금이체 동의(agree_yn = 'Y') 회원만 대상
                List<Map<String, Object>> agreed = new ArrayList<>();
                for (Map<String, Object> mm : members) {
                    String ay = mm.get("agree_yn") != null ? mm.get("agree_yn").toString() : "N";
                    if ("Y".equals(ay)) agreed.add(mm);
                }
                if (agreed.isEmpty()) {
                    cnt[3]++;
                    details.add(detail(rn, null, "FAILED", "출금이체 동의가 완료된 회원이 없습니다: "
                            + (StringUtils.hasText(memberNo) ? memberNo : memberName)));
                    continue;
                }

                final String billingYm  = deductDate.substring(0, 6);
                final String fDeductDate = deductDate;
                final String fSendDateN  = calcSendDate(deductDate, effDeductType);
                final Long fAmountN      = billingAmount;
                final String fMemoN      = memo;

                // 동명이인이면 매칭된 회원 수만큼 각각 청구 생성
                for (Map<String, Object> mm : agreed) {
                    final Long memberId        = ((Number) mm.get("id")).longValue();
                    final String useMemberName = mm.get("member_name") != null ? mm.get("member_name").toString() : memberName;
                    final String useBankCode   = mm.get("bank_code") != null ? mm.get("bank_code").toString() : null;
                    final String useBankAccount = mm.get("bank_account") != null ? mm.get("bank_account").toString() : null;
                    final String useHolder     = mm.get("account_holder") != null ? mm.get("account_holder").toString() : useMemberName;
                    final String useDeductDay  = mm.get("deduct_day") != null ? mm.get("deduct_day").toString() : null;
                    cnt[0]++;
                    writes.add(() -> {
                        String newSeq = generateBillingSeq(spjangcd, billingYm);
                        sqlRunner.execute("""
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
                                    :deductType, 'PENDING', :memo,
                                    :userId, NOW(), :userId, NOW()
                                )
                                """, new MapSqlParameterSource()
                                .addValue("spjangcd", spjangcd).addValue("billingYm", billingYm).addValue("billingSeq", newSeq)
                                .addValue("memberId", memberId).addValue("memberName", useMemberName)
                                .addValue("bankCode", useBankCode).addValue("bankAccount", useBankAccount)
                                .addValue("accountHolder", useHolder).addValue("billingAmount", fAmountN)
                                .addValue("deductDay", useDeductDay).addValue("deductDate", fDeductDate).addValue("sendDate", fSendDateN)
                                .addValue("deductType", effDeductType).addValue("memo", fMemoN).addValue("userId", userId));
                    });
                }
                if (agreed.size() > 1) {
                    details.add(detail(rn, null, "INSERTED",
                            "동명이인 " + agreed.size() + "명 → " + agreed.size() + "건 생성 예정 (" + memberName + ")"));
                } else {
                    details.add(detail(rn, null, "INSERTED",
                            "신규 등록 예정 (" + agreed.get(0).get("member_name") + ")"));
                }
            }
        }

        // ── 2단계: 실패가 하나라도 있으면 전체 취소(쓰기 없음), 없으면 일괄 반영 ──
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("total", rows.size());
        out.put("details", details);

        if (cnt[3] > 0) {
            // 원자성: 실패 행이 있으면 아무것도 반영하지 않음
            out.put("applied", false);
            out.put("inserted", 0);
            out.put("updated", 0);
            out.put("skipped", cnt[2]);
            out.put("failed", cnt[3]);
            out.put("plan_inserted", cnt[0]);
            out.put("plan_updated", cnt[1]);
            return out;
        }

        for (Runnable w : writes) w.run();

        out.put("applied", true);
        out.put("inserted", cnt[0]);
        out.put("updated", cnt[1]);
        out.put("skipped", cnt[2]);
        out.put("failed", 0);
        return out;
    }

    private Map<String, Object> detail(int rowNo, String billingSeq, String resultCode, String message) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("row", rowNo);
        m.put("billing_seq", billingSeq);
        m.put("result", resultCode);
        m.put("message", message);
        return m;
    }

    /** 은행명 → 은행코드 조회 (회원에 코드가 없을 때 보완용) */
    private String resolveBankCode(String bankName) {
        if (!StringUtils.hasText(bankName)) return null;
        try {
            Map<String, Object> r = sqlRunner.getRow(
                    "SELECT bank_code FROM cms_bank_code WHERE bank_name = :bankName LIMIT 1",
                    new MapSqlParameterSource().addValue("bankName", bankName.trim()));
            return r != null && r.get("bank_code") != null ? r.get("bank_code").toString() : null;
        } catch (Exception e) {
            log.warn("[bulkUpsertBilling] 은행코드 조회 실패: {}", bankName);
            return null;
        }
    }

    /** 청구 자동생성 */
    @Transactional
    public Map<String, Object> generateBilling(String billingYm, String deductType, String userId) {
        String spjangcd = TenantContext.get();

        YearMonth ym = YearMonth.parse(billingYm, DateTimeFormatter.ofPattern("yyyyMM"));
        String firstDay = ym.atDay(1).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String lastDay  = ym.atEndOfMonth().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String monthStr = String.valueOf(ym.getMonthValue());

        var param = new org.springframework.jdbc.core.namedparam.MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);
        param.addValue("firstDay", firstDay);
        param.addValue("lastDay", lastDay);
        param.addValue("monthStr", monthStr);

        // 자동생성 대상 납부자 조회 — 이력으로 회원을 막지 않는다 (NOT EXISTS 제거)
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
            ORDER BY m.id
            """;

        List<Map<String, Object>> members = sqlRunner.getRows(memberSql, param);

        if (members.isEmpty()) {
            Map<String, Object> result = new HashMap<>();
            result.put("count", 0);
            return result;
        }

        int nextSeq = getNextBillingSeqNo(spjangcd, billingYm);

        int count = 0;
        int skippedCount = 0;
        java.time.LocalDate today = java.time.LocalDate.now();
        int nowHour = java.time.LocalTime.now().getHour();
        String todayStr = today.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String effectiveDeductType = deductType != null ? deductType : "EB";

        for (Map<String, Object> m : members) {
            String deductDay = (String) m.get("deduct_day");
            if (deductDay == null || deductDay.isEmpty()) { skippedCount++; continue; }

            String deductDate = "99".equals(deductDay) ? lastDay : billingYm + deductDay;
            deductDate = cmsHolidayService.getNextBusinessDay(deductDate);

            // 오늘 이전 날짜 스킵
            if (deductDate.compareTo(todayStr) < 0) { skippedCount++; continue; }
            if ("EB".equals(effectiveDeductType)) {
                String deadlineDay = cmsHolidayService.getPrevBusinessDay(
                        LocalDate.parse(deductDate, DateTimeFormatter.ofPattern("yyyyMMdd"))
                                .minusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMdd")));
                // (원본 유지) 마감 지난 건 자동에서는 막지 않음. 통일하려면 아래 한 줄 추가:
                // if (deadlineDay.compareTo(todayStr) < 0) { skippedCount++; continue; }
                if (deadlineDay.equals(todayStr) && nowHour >= 17) { skippedCount++; continue; }
            }
            if ("EC".equals(effectiveDeductType) && deductDate.equals(todayStr) && nowHour >= 11) { skippedCount++; continue; }

            // 중지 기간 체크
            if (isPausedInBillingMonth(m, ym)) {
                log.info("[generateBilling] 중지 기간 → 청구 생성 스킵 - memberId: {}", m.get("id"));
                skippedCount++;
                continue;
            }

            // ⭐ 중복 판정: '이번 출금일(deduct_date)'에 유효한 청구가 있을 때만 스킵
            Map<String, Object> dup = sqlRunner.getRow("""
            SELECT 1 FROM cms_billing
            WHERE spjangcd    = :spjangcd
              AND member_id   = :memberId
              AND deduct_type = :deductType
              AND deduct_date = :deductDate
              AND status IN ('PENDING','REQUESTED','SUCCESS')
            LIMIT 1
            """,
                    new org.springframework.jdbc.core.namedparam.MapSqlParameterSource()
                            .addValue("spjangcd",   spjangcd)
                            .addValue("memberId",   ((Number) m.get("id")).longValue())
                            .addValue("deductType", effectiveDeductType)
                            .addValue("deductDate", deductDate));
            if (dup != null) { skippedCount++; continue; }

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
            ip.addValue("deductType",    effectiveDeductType);
            ip.addValue("userId",        userId);
            ip.addValue("sendDate",      calcSendDate(deductDate, effectiveDeductType));

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
    public Map<String, Object> getBillingResultList(String billingYm, String deductDateFrom, String deductDateTo,
                                                    String resultDate, String status,
                                                    String memberName, String deductType, int page, int size) {
        String spjangcd = TenantContext.get();
        var param = new org.springframework.jdbc.core.namedparam.MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);
        param.addValue("deductType", deductType != null ? deductType : "EB");

        String baseWhere =
                "  FROM cms_billing b" +
                        "  LEFT JOIN cms_bank_code bc ON bc.bank_code = b.bank_code" +
                        "  LEFT JOIN cms_member m ON m.id = b.member_id" +
                        "  WHERE b.spjangcd = :spjangcd" +
                        "    AND b.deduct_type = :deductType";

        String filters = "";
        // 출금일자(deduct_date) 범위 필터 — 신규 기본 조회 방식
        if (StringUtils.hasText(deductDateFrom) && StringUtils.hasText(deductDateTo)) {
            filters += " AND b.deduct_date BETWEEN :ddFrom AND :ddTo";
            param.addValue("ddFrom", deductDateFrom);
            param.addValue("ddTo",   deductDateTo);
        } else if (StringUtils.hasText(deductDateFrom)) {
            filters += " AND b.deduct_date >= :ddFrom";
            param.addValue("ddFrom", deductDateFrom);
        } else if (StringUtils.hasText(deductDateTo)) {
            filters += " AND b.deduct_date <= :ddTo";
            param.addValue("ddTo", deductDateTo);
        } else if (StringUtils.hasText(billingYm)) {
            // 하위호환: 범위가 없고 청구년월만 오면 기존처럼 월 단위로 조회
            filters += " AND LEFT(b.deduct_date, 6) = :billingYm";
            param.addValue("billingYm", billingYm);
        }
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
                "SELECT b.id, b.billing_seq, b.member_name, m.id_number, m.biz_no, m.resident_no," +
                        "       b.bank_code, bc.bank_name, b.bank_account, b.billing_amount," +
                        "       b.deduct_date, b.status, b.result_code, b.result_msg, b.result_date," +
                        "       b.memo," +
                        "       b.fee_request, b.fee_success," +
                        "       CASE WHEN b.status='SUCCESS' THEN b.fee_request+b.fee_success" +
                        "            WHEN b.status='FAIL'    THEN b.fee_request ELSE 0 END AS fee_total," +
                        "       CASE WHEN EXISTS (SELECT 1 FROM cms_billing rb WHERE rb.spjangcd=b.spjangcd" +
                        "              AND rb.id > b.id AND rb.status NOT IN ('CANCEL','FAIL','ERROR')" +
                        "              AND ( (b.erp_mis_key IS NOT NULL AND rb.erp_mis_key = b.erp_mis_key)" +
                        "                 OR (b.erp_mis_key IS NULL AND rb.member_id=b.member_id" +
                        "                     AND rb.memo LIKE '%불능 / 재청구%' AND rb.deduct_date > b.deduct_date) )" +
                        "              ) THEN 'Y' ELSE 'N' END AS recharged_yn" +
                        baseWhere + filters + " ORDER BY b.deduct_date DESC, b.billing_seq LIMIT :pgSize OFFSET :pgOffset";

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
          AND LEFT(b.deduct_date, 6) = :billingYm
          AND b.deduct_type = :deductType
          AND b.status      = 'PENDING'
          AND b.send_date  IS NOT NULL
        GROUP BY b.deduct_date, b.send_date
        ORDER BY b.deduct_date
        """, param);
    }

    /**
     * ERP(TB_DA023 미수원장) 미수금 후보를 조회해서 cms_member와 매칭한 목록을 리턴한다. (INSERT 안 함)
     * 모달에서 사용자가 선택할 목록을 만드는 용도.
     * 각 행 status: OK(생성 가능) / DUP(이미 청구됨) / NOT_AGREED(미인증)
     * ※ cms_member 미등록 건(구 NO_MEMBER)은 목록에서 제외한다.
     */
    public List<Map<String, Object>> previewErpBilling(String billingYm) {
        return previewErpBilling(billingYm, "REP");
    }

    public List<Map<String, Object>> previewErpBilling(String billingYm, String nameTypeParam) {
        String spjangcd = TenantContext.get();

        Map<String, Object> erp = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT host, port, db_name, username, password, custcd FROM tb_xa012_erp WHERE spjangcd = :spjangcd",
                new MapSqlParameterSource("spjangcd", spjangcd));
        if (erp == null) throw new IllegalStateException("ERP 접속정보가 없습니다.");

        String custcd = str(erp.get("custcd"));
        String dbUrl = String.format("jdbc:sqlserver://%s:%s;databaseName=%s;encrypt=false",
                str(erp.get("host")), str(erp.get("port")), str(erp.get("db_name")));

        // 표시명(SITE=현장명 / REP=대표거래처명). 화면에서 고른 값을 그대로 쓴다.
        //  예전에는 "REP" 하드코딩이라 화면에서 현장명을 골라도 거래처명이 저장됐다.
        String nameType = "SITE".equalsIgnoreCase(nameTypeParam) ? "SITE" : "REP";


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

            // 기준 테이블: TB_DA023(미수 원장). 파워빌더 PROC_CMS_R mode 03/04 와 동일 조건.
            //  - 유지보수(관리) 매출만  : gubun IN (TB_DA020 where jgubun='1')
            //  - 이미 입금된 건 제외    : TB_DA026 수납분 제외 + 잔액>0(부분입금 반영)
            //  - 이관 전 CMS 출금 성공분 제외 : TB_CMSEB21 / TB_CMSEC21 ENDFLAG='Y'
            //  - 이월(bemisdate)/상계(sangdate) 건 제외
            String sql = """
                SELECT
                    A.cltcd,
                    A.actcd,
                    A.misdate,
                    A.misnum,
                    C.cltnm                           AS member_name_rep,
                    F.actnm                           AS member_name_site,
                    COALESCE(NULLIF(LTRIM(RTRIM(C.autoflag)),''),
                             NULLIF(LTRIM(RTRIM(E.autoflag)),'')) AS autoflag,
                    (A.misamt - ISNULL(A.bamt,0)   - ISNULL(A.jamt,0)  - ISNULL(A.sunamt,0)
                              - ISNULL(A.gamt,0)   - ISNULL(A.csamt,0) - ISNULL(A.cmar,0)
                              - ISNULL(A.dcamt,0)) AS billing_amount
                FROM TB_DA023 A WITH(NOLOCK)
                INNER JOIN TB_XCLIENT C WITH(NOLOCK)
                    ON A.cltcd = C.cltcd
                    AND C.custcd = ?
                LEFT JOIN TB_E601 F WITH(NOLOCK)
                    ON F.custcd = A.custcd
                    AND F.actcd = A.actcd
                OUTER APPLY (
                    SELECT TOP 1 e.accnum, e.cmsrnum, e.autoflag
                    FROM TB_E101 e WITH(NOLOCK)
                    WHERE e.custcd = A.custcd AND e.actcd = A.actcd
                      AND e.contg <> '04'
                      AND NULLIF(LTRIM(RTRIM(e.accnum)),'') IS NOT NULL
                    ORDER BY e.stdate DESC
                ) E
                WHERE A.custcd = ?
                AND LEFT(A.misdate, 6) IN (?, ?)
                AND A.gubun IN (SELECT artcd FROM TB_DA020 WITH(NOLOCK) WHERE jgubun = '1')
                AND A.misdate + A.misnum NOT IN (
                        SELECT misdate + misnum FROM TB_DA026 WITH(NOLOCK))
                AND A.misdate + A.misnum NOT IN (
                        SELECT misdate + misnum FROM TB_CMSEB21 WITH(NOLOCK) WHERE ENDFLAG = 'Y')
                AND A.misdate + A.misnum NOT IN (
                        SELECT misdate + misnum FROM TB_CMSEC21 WITH(NOLOCK) WHERE ENDFLAG = 'Y')
                AND (A.bemisdate + A.bemisnum IS NULL OR LEN(A.bemisdate + A.bemisnum) = 0)
                AND (A.sangdate IS NULL OR LEN(ISNULL(A.sangdate,'')) = 0)
                AND (A.misamt - ISNULL(A.bamt,0)   - ISNULL(A.jamt,0)  - ISNULL(A.sunamt,0)
                              - ISNULL(A.gamt,0)   - ISNULL(A.csamt,0) - ISNULL(A.cmar,0)
                              - ISNULL(A.dcamt,0)) > 0
                AND ( NULLIF(LTRIM(RTRIM(C.accnum)),'')  IS NOT NULL
                   OR NULLIF(LTRIM(RTRIM(E.accnum)),'')  IS NOT NULL )
                AND ( NULLIF(LTRIM(RTRIM(C.cmsrnum)),'') IS NOT NULL
                   OR NULLIF(LTRIM(RTRIM(E.cmsrnum)),'') IS NOT NULL )
                -- ★ ERP에서 CMS 자동이체를 쓰는 건만 청구 대상이다.
                --   거래처 단위는 XCLIENT.allchk=1, 현장 단위는 E101.cmsflag=1.
                --   이 조건이 없으면 ERP에서 CMS 를 끔 거래처도 cms_member 가 남아 있는 한
                --   계속 추천돼 생성된다. (미르에셋·효진기공·헤리티지1 사례)
                AND ( ISNULL(C.allchk, 0) = 1
                   OR EXISTS (SELECT 1 FROM TB_E101 e2 WITH(NOLOCK)
                               WHERE e2.custcd = A.custcd AND e2.actcd = A.actcd
                                 AND e2.cmsflag = 1 AND ISNULL(e2.contg,'') <> '04') )
                ORDER BY A.cltcd, A.misdate
                """;

            String prevYm = prevYyyymm(billingYm);

            // ★ 회원은 한 번만 읽어 메모리 인덱스로 쓴다.
            //   예전에는 ERP 미수 한 건마다 cms_member 를 조회해서, 미수가 수백 건이면
            //   그만큼 쿼리가 나가 미리보기가 느렸다.
            //   actcd(현장) / cltcd(거래처) 는 코드 체계가 달라 번호대가 겹치므로 맵을 분리한다.
            //   현장 회원은 소속 거래처의 cltcd 도 갖고 있으니 byCltcd 에는 넣지 않는다.
            Map<String, Map<String, Object>> memberByActcd = new HashMap<>();
            Map<String, Map<String, Object>> memberByCltcd = new HashMap<>();
            for (Map<String, Object> m : sqlRunner.getRows(/* skip_tenant_check */
                    """
                    SELECT id, member_name, member_no, bank_code, bank_account,
                           account_holder, deduct_day, agree_yn, deduct_month_type,
                           actcd, cltcd
                    FROM cms_member
                    WHERE spjangcd = :spjangcd AND status = 'ACTIVE'
                    ORDER BY CASE WHEN agree_yn = 'Y' THEN 0 ELSE 1 END, id
                    """,
                    new MapSqlParameterSource("spjangcd", spjangcd))) {
                String mAct = str(m.get("actcd"));
                String mClt = str(m.get("cltcd"));
                if (StringUtils.hasText(mAct))       memberByActcd.putIfAbsent(mAct, m);
                else if (StringUtils.hasText(mClt))  memberByCltcd.putIfAbsent(mClt, m);
            }

            try (java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, custcd);
                ps.setString(2, custcd);
                ps.setString(3, billingYm);
                ps.setString(4, prevYm);

                try (java.sql.ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String cltcd         = rs.getString("cltcd");
                        String actcd         = rs.getString("actcd");
                        String misdate       = rs.getString("misdate");
                        String misnum        = rs.getString("misnum");
                        String misKey        = misdate + misnum;
                        long   billingAmount = rs.getLong("billing_amount");   // DA023 잔액(부분입금 차감 후)
                        String autoflag      = str(rs.getString("autoflag"));
                        String misYm         = (misdate != null && misdate.length() >= 6) ? misdate.substring(0, 6) : "";
                        String nameRep       = rs.getString("member_name_rep");   // 대표거래처명(XCLIENT.cltnm)
                        String nameSite      = rs.getString("member_name_site");  // 현장명(E601.actnm)
                        if (!StringUtils.hasText(nameSite)) nameSite = nameRep;    // 현장명 없으면 대표명
                        if (!StringUtils.hasText(nameRep))  nameRep  = nameSite;
                        // 기관 기본값(SITE/REP)에 따른 기본 표시명. 화면 토글이 site/rep로 전환.
                        String memberName    = "SITE".equalsIgnoreCase(nameType) ? nameSite : nameRep;

                        if (billingAmount <= 0) continue;

                        // 현장(actcd) 우선 → 없으면 거래처(cltcd).
                        //  한 거래처 밑에 현장이 여럿일 수 있고(지원에셋 00811 ← CS메디컬 00779 +
                        //  검단센트럴시티 00756), 현장에 CMS 회원이 있으면 그 회원이,
                        //  없으면(cmsflag 미사용) 거래처가 대납하는 구조다.
                        Map<String, Object> member = null;
                        if (StringUtils.hasText(actcd)) member = memberByActcd.get(actcd);
                        if (member == null && StringUtils.hasText(cltcd)) member = memberByCltcd.get(cltcd);

                        // 당월/익월 판단: cms_member.deduct_month_type 우선, 없으면 ERP XCLIENT.autoflag 폴백
                        //  익월(NEXT / autoflag=2) → 전월 발생 미수를 이번 달 청구, 그 외 → 당월 미수
                        String monthType = (member != null && StringUtils.hasText(str(member.get("deduct_month_type"))))
                                ? str(member.get("deduct_month_type"))
                                : ("2".equals(autoflag) ? "NEXT" : "CURRENT");
                        String expectYm  = "NEXT".equalsIgnoreCase(monthType) ? prevYm : billingYm;
                        if (!expectYm.equals(misYm)) continue;

                        String rowStatus;
                        Object memberId = null;
                        String deductDay = "", deductDate = "";
                        String dispName = memberName;
                        String bankCode = "", bankAccount = "", accountHolder = "";

                        // ★ cms_member 에 매칭되는 납부자가 없는 건은 목록에서 아예 제외한다.
                        //   CMS 대상이 아닌 ERP 거래처까지 노출되어 "이건 왜 나오냐"는 혼선이 생김.
                        //   (생성 대상이 아니므로 제외해도 결과는 동일하다.)
                        if (member == null) continue;

                        // ★ 표시명은 실제 청구가 들어갈 납부자(cms_member) 기준으로 낸다.
                        //   ERP 원본명(거래처/현장)을 그대로 쓰면, 한 거래처 밑 현장이 여럿일 때
                        //   서로 다른 납부자로 나가는 건이 같은 이름으로 보여 구분이 안 된다.
                        if (StringUtils.hasText(str(member.get("member_name")))) {
                            dispName = str(member.get("member_name"));
                        }

                        if (existingMisKeys.contains(misKey)) {
                            rowStatus = "DUP";
                        } else if (!"Y".equals(str(member.get("agree_yn")))) {
                            rowStatus = "NOT_AGREED";
                            memberId  = member.get("id");
                        } else {
                            rowStatus     = "OK";
                            memberId      = member.get("id");
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
                        row.put("member_name",      dispName);
                        row.put("member_name_rep",  nameRep);
                        row.put("member_name_site", nameSite);
                        row.put("billing_amount", billingAmount);
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
    /** 구버전 시그니처 유지 — 출금일 일괄지정 없이 호출하는 경로용. */
    @Transactional
    public Map<String, Object> createErpBilling(String billingYm, String sendDate,
                                                List<String> selectedKeys, String userId) {
        return createErpBilling(billingYm, sendDate, selectedKeys, null, userId);
    }

    /**
     * @param deductDateOverride 출금일 일괄지정(yyyyMMdd). 비어 있으면 납부자별 약정일로 자동 계산.
     *        전월치를 뒤늦게 청구할 때 약정일이 이미 지난 날짜가 되므로 화면에서 지정할 수 있게 한다.
     */
    @Transactional
    public Map<String, Object> createErpBilling(String billingYm, String sendDate,
                                                List<String> selectedKeys,
                                                String deductDateOverride, String userId) {
        return createErpBilling(billingYm, sendDate, selectedKeys, deductDateOverride, userId, "REP");
    }

    /**
     * @param nameTypeParam 청구건에 저장할 표시명. SITE=현장명, REP=대표거래처명(기본).
     */
    @Transactional
    public Map<String, Object> createErpBilling(String billingYm, String sendDate,
                                                List<String> selectedKeys,
                                                String deductDateOverride, String userId,
                                                String nameTypeParam) {
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

        // 출금일 일괄지정: 휴일이면 다음 영업일로 보정한다.
        String fixedDeductDate = null;
        if (StringUtils.hasText(deductDateOverride)) {
            fixedDeductDate = cmsHolidayService.getNextBusinessDay(deductDateOverride.replace("-", ""));
        }

        Set<String> wanted = new HashSet<>(selectedKeys);

        // preview로 후보를 다시 만들어 선택분만 신뢰성 있게 생성 (금액/회원 재검증)
        List<Map<String, Object>> candidates = previewErpBilling(billingYm, nameTypeParam);

        for (Map<String, Object> c : candidates) {
            String misKey = str(c.get("erp_mis_key"));
            if (!wanted.contains(misKey)) continue;

            String rowStatus = str(c.get("row_status"));
            String memberName = str(c.get("member_name"));

            if (!"OK".equals(rowStatus)) {
                // DUP / NOT_AGREED 는 생성 안 함
                notFound.add(Map.of("cltcd", str(c.get("cltcd")),
                        "member_name", memberName + "(" + rowStatus + ")"));
                skipped++;
                continue;
            }

            String deductDate = StringUtils.hasText(fixedDeductDate)
                    ? fixedDeductDate                       // 화면에서 일괄지정한 출금일
                    : str(c.get("deduct_date"));            // 납부자별 약정일 기준 자동계산
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

    /** YYYYMM의 전월 YYYYMM 반환 (익월 청구 거래처의 전월 미수 조회용) */
    private String prevYyyymm(String yyyymm) {
        if (yyyymm == null || yyyymm.length() < 6) return yyyymm;
        try {
            int y = Integer.parseInt(yyyymm.substring(0, 4));
            int m = Integer.parseInt(yyyymm.substring(4, 6));
            m--; if (m == 0) { m = 12; y--; }
            return String.format("%04d%02d", y, m);
        } catch (NumberFormatException e) {
            return yyyymm;
        }
    }

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

        // 청구 가능한 납부자 조회 — 이력으로 회원을 막지 않는다 (NOT EXISTS 제거)
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
    ORDER BY m.deduct_day
    """,
                new MapSqlParameterSource()
                        .addValue("spjangcd",   spjangcd)
                        .addValue("firstDay",   firstDay)
                        .addValue("lastDay",    lastDay)
                        .addValue("monthStr",   monthStr));

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

            // ⭐ 이번 출금일에 이미 유효한 청구(PENDING/REQUESTED/SUCCESS)가 있으면 카운트 제외
            //    (생성 로직과 동일 기준 — FAIL/ERROR/CANCEL은 재청구 대상이므로 카운트에 포함)
            Map<String, Object> dup = sqlRunner.getRow("""
            SELECT 1 FROM cms_billing
            WHERE spjangcd    = :spjangcd
              AND member_id   = :memberId
              AND deduct_type = :deductType
              AND deduct_date = :deductDate
              AND status IN ('PENDING','REQUESTED','SUCCESS')
            LIMIT 1
            """,
                    new MapSqlParameterSource()
                            .addValue("spjangcd",   spjangcd)
                            .addValue("memberId",   ((Number) m.get("id")).longValue())
                            .addValue("deductType", effectiveDeductType)
                            .addValue("deductDate", deductDate));
            if (dup != null) continue;

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
                    reason = "오늘 17시 마감 초과";
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

        // 대상 납부자 조회 — 이력으로 회원을 막지 않는다 (NOT EXISTS 제거)
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
    ORDER BY m.deduct_day, m.id
    """,
                new MapSqlParameterSource()
                        .addValue("spjangcd",   spjangcd)
                        .addValue("firstDay",   firstDay)
                        .addValue("lastDay",    lastDay)
                        .addValue("monthStr",   monthStr)
                        .addValue("deductDays", normalizedDays.toArray(new String[0])));

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

            // ⭐ 중복 판정: billing_ym이 아니라 '이번 출금일(deduct_date)'에
            //    유효한 청구(PENDING/REQUESTED/SUCCESS)가 있을 때만 스킵.
            //    FAIL/ERROR/CANCEL은 유효하지 않으므로 재생성 허용.
            Map<String, Object> dup = sqlRunner.getRow("""
            SELECT 1 FROM cms_billing
            WHERE spjangcd    = :spjangcd
              AND member_id   = :memberId
              AND deduct_type = :deductType
              AND deduct_date = :deductDate
              AND status IN ('PENDING','REQUESTED','SUCCESS')
            LIMIT 1
            """,
                    new MapSqlParameterSource()
                            .addValue("spjangcd",   spjangcd)
                            .addValue("memberId",   ((Number) m.get("id")).longValue())
                            .addValue("deductType", effectiveDeductType)
                            .addValue("deductDate", deductDate));
            if (dup != null) { skippedCount++; continue; }

            String billingSeq = billingYm + "-" + String.format("%04d", nextSeq++);

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

    /**
     * 통장기재내용 접미어 저장.
     * EB21 통장기재내용(16bytes)은 [기관 등록문구 + 접미어] 로 만들어진다.
     * 등록문구는 결제원에 신고된 값이라 바꿀 수 없으므로, 남는 바이트에만 접미어를 넣는다.
     *  - 전송 전(PENDING) 건만 수정 가능. 이미 나간 건은 바꿔도 통장에 반영되지 않는다.
     *  - 빈 값이면 NULL 로 지워 기본 문구만 나가게 한다.
     */
    @Transactional
    public int changePrintSuffix(String ids, String printSuffix) {
        String spjangcd = TenantContext.get();
        List<Long> idList = Arrays.stream(ids.split(","))
                .map(String::trim).map(Long::parseLong).collect(Collectors.toList());

        // ★ 통장기재내용이 한글모드(H)면 숫자·영문·공백도 2바이트(전각)여야 한다.
        //   반각이 섞이면 은행이 불능코드 0087(한글전용 필드에 이외의 값)로 거부한다.
        //   화면에서도 변환하지만, API 직접 호출 등을 대비해 서버에서 한 번 더 강제한다.
        String suffix = StringUtils.hasText(printSuffix) ? toFullWidth(printSuffix.trim()) : null;

        // 기관 등록문구 + 접미어가 16바이트(EUC-KR)를 넘으면 은행에서 잘리거나 불능될 수 있다.
        if (suffix != null) {
            Map<String, Object> cms = sqlRunner.getRow(/* skip_tenant_check */
                    "SELECT cms_description FROM tb_xa012_cms WHERE spjangcd = :spjangcd LIMIT 1",
                    new MapSqlParameterSource("spjangcd", spjangcd));
            String desc = cms != null ? str(cms.get("cms_description")) : "";
            int used = eucKrLength(desc) + eucKrLength(suffix);
            if (used > 16) {
                throw new IllegalArgumentException(
                        "통장기재내용이 16바이트를 초과합니다. (기본문구 " + eucKrLength(desc)
                                + "바이트 + 입력 " + eucKrLength(suffix) + "바이트)");
            }
        }

        return sqlRunner.execute(/* skip_tenant_check */
                """
                UPDATE cms_billing
                SET print_suffix = :suffix,
                    _modified    = NOW()
                WHERE id = ANY(:ids::BIGINT[])
                  AND spjangcd = :spjangcd
                  AND status = 'PENDING'
                """,
                new MapSqlParameterSource()
                        .addValue("ids", idList.toArray(new Long[0]))
                        .addValue("suffix", suffix)
                        .addValue("spjangcd", spjangcd));
    }

    /** 반각 영숫자·공백을 전각으로 변환 (통장기재내용 한글모드 대응) */
    private String toFullWidth(String s) {
        if (s == null || s.isEmpty()) return s;
        StringBuilder sb = new StringBuilder(s.length());
        for (char c : s.toCharArray()) {
            if (c == ' ')                  sb.append('\u3000');
            else if (c >= '!' && c <= '~')  sb.append((char) (c + 0xFEE0));
            else                            sb.append(c);
        }
        return sb.toString();
    }

    /** EUC-KR 기준 바이트 길이 (한글·전각 2, 영숫자 1) */
    private int eucKrLength(String s) {
        if (s == null || s.isEmpty()) return 0;
        try { return s.getBytes("EUC-KR").length; }
        catch (java.io.UnsupportedEncodingException e) { return s.length() * 2; }
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