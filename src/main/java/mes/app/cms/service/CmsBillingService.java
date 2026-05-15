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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class CmsBillingService {

    @Autowired
    SqlRunner sqlRunner;

    @Autowired
    CmsHolidayService cmsHolidayService;

    /** 청구 목록 조회 */
    public List<Map<String, Object>> getBillingList(String billingYm, String sendDate, String memberName, String status, String deductType) {
        String spjangcd = TenantContext.get();
        var param = new org.springframework.jdbc.core.namedparam.MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);
        param.addValue("billingYm", billingYm);
        param.addValue("deductType", deductType != null ? deductType : "EB");

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
                 , b._created
                 , b._modified
                 , CASE WHEN EXISTS (
                    SELECT 1 FROM cms_billing rb
                    WHERE rb.spjangcd = b.spjangcd
                      AND rb.member_id = b.member_id
                      AND rb.memo LIKE '%불능 / 재청구%'
                      AND rb.status NOT IN ('CANCEL', 'FAIL', 'ERROR')
                ) THEN 'Y' ELSE 'N' END AS recharged_yn
            FROM cms_billing b
            LEFT JOIN cms_bank_code bc ON bc.bank_code = b.bank_code
            WHERE b.spjangcd    = :spjangcd
              AND b.billing_ym  = :billingYm
              AND b.deduct_type = :deductType
            """;

        if (StringUtils.hasText(sendDate)) {
            sql += """
                 AND b.send_date = :sendDate
                """;
            param.addValue("sendDate", sendDate);
        }
        if (StringUtils.hasText(memberName)) {
            sql += " AND b.member_name LIKE '%' || :memberName || '%'";
            param.addValue("memberName", memberName);
        }
        if (StringUtils.hasText(status)) {
            sql += " AND b.status = :status";
            param.addValue("status", status);
        }

        sql += " ORDER BY b.billing_seq";
        return sqlRunner.getRows(sql, param);
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

    /** 청구 저장 (신규/수정) */
    public Long saveBilling(Long id, String billingYm, String memberId,
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
            if (StringUtils.hasText(memberId) && StringUtils.hasText(deductDate)) {
                List<Map<String, Object>> dup = sqlRunner.getRows(/* skip_tenant_check */
                        """
                        SELECT 1 FROM cms_billing
                        WHERE spjangcd  = :spjangcd
                          AND member_id = :memberId
                          AND deduct_date = :deductDate
                          AND status NOT IN ('CANCEL', 'FAIL', 'ERROR')
                        """,
                        new org.springframework.jdbc.core.namedparam.MapSqlParameterSource()
                                .addValue("spjangcd",   spjangcd)
                                .addValue("memberId",   Long.parseLong(memberId))
                                .addValue("deductDate", deductDate));
                if (!dup.isEmpty()) {
                    log.warn("[saveBilling] 중복 청구 차단 memberId={} deductDate={}", memberId, deductDate);
                    throw new IllegalStateException("동일한 출금일에 이미 청구가 존재합니다.");
                }
            }

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
                FROM cms_member m
                WHERE m.spjangcd     = :spjangcd
                  AND m.status       = 'ACTIVE'
                  AND m.agree_yn     = 'Y'
                  AND m.agree_date IS NOT NULL
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
            deductDate = cmsHolidayService.getNextBusinessDay(deductDate);

            // 오늘 이전 날짜 스킵
            if (deductDate.compareTo(todayStr) < 0) { skippedCount++; continue; }
            if ("EB".equals(effectiveDeductType)) {
                String deadlineDay = cmsHolidayService.getPrevBusinessDay(
                        LocalDate.parse(deductDate, DateTimeFormatter.ofPattern("yyyyMMdd"))
                                .minusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMdd")));
                if (deadlineDay.equals(todayStr) && nowHour >= 15) { skippedCount++; continue; }
            }
            if ("EC".equals(effectiveDeductType) && deductDate.equals(todayStr) && nowHour >= 11) { skippedCount++; continue; }

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
    public List<Map<String, Object>> getBillingResultList(String billingYm, String resultDate, String status, String memberName, String deductType) {
        String spjangcd = TenantContext.get();
        var param = new org.springframework.jdbc.core.namedparam.MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);
        param.addValue("billingYm", billingYm);
        param.addValue("deductType", deductType != null ? deductType : "EB");

        String sql = """
            SELECT b.id
                 , b.billing_seq
                 , b.member_name
                 , b.bank_code
                 , bc.bank_name
                 , b.bank_account
                 , b.billing_amount
                 , b.deduct_date
                 , b.status
                 , b.result_code
                 , b.result_msg
                 , b.result_date
                 , b.fee_request
                 , b.fee_success
                 , CASE
                     WHEN b.status = 'SUCCESS' THEN b.fee_request + b.fee_success
                     WHEN b.status = 'FAIL'    THEN b.fee_request
                     ELSE 0
                   END AS fee_total
                 , CASE WHEN EXISTS (
                        SELECT 1 FROM cms_billing rb
                        WHERE rb.spjangcd = b.spjangcd
                          AND rb.member_id = b.member_id
                          AND rb.memo LIKE '%불능 / 재청구%'
                          AND rb.status NOT IN ('CANCEL', 'FAIL', 'ERROR')
                    ) THEN 'Y' ELSE 'N' END AS recharged_yn
            FROM cms_billing b
            LEFT JOIN cms_bank_code bc ON bc.bank_code = b.bank_code
            WHERE b.spjangcd    = :spjangcd
              AND b.billing_ym  = :billingYm
              AND b.deduct_type = :deductType
            """;

        if (StringUtils.hasText(resultDate)) {
            sql += " AND (b.result_date = :resultDate OR (b.status = 'FAIL' AND b.deduct_date = :resultDate))";
            param.addValue("resultDate", resultDate);
        }
        if (StringUtils.hasText(status)) {
            sql += " AND b.status = :status";
            param.addValue("status", status);
        } else {
            sql += " AND b.status IN ('SUCCESS', 'FAIL')";
        }
        if (StringUtils.hasText(memberName)) {
            sql += " AND b.member_name LIKE '%' || :memberName || '%'";
            param.addValue("memberName", memberName);
        }

        sql += " ORDER BY b.billing_seq";
        return sqlRunner.getRows(sql, param);
    }

    /** 불능 건 재청구 — FAIL 상태 건을 납부자 현재 정보 기준으로 새 PENDING 생성 */
    @Transactional
    public Map<String, Object> rechargeBilling(List<Long> ids, List<String> deductDates, String userId) {
        String spjangcd = TenantContext.get();
        int count = 0;

        for (int i = 0; i < ids.size(); i++) {
            Long id = ids.get(i);
            String newDeductDate = deductDates.size() > i ? deductDates.get(i) : null;
            var pOrig = new org.springframework.jdbc.core.namedparam.MapSqlParameterSource();
            pOrig.addValue("id", id);
            pOrig.addValue("spjangcd", spjangcd);

            Map<String, Object> orig = sqlRunner.getRow("""
                SELECT billing_ym, member_id, member_name,
                       bank_code, bank_account, account_holder,
                       billing_amount, deduct_day, deduct_date, result_code, deduct_type
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
            String deductDay     = (String) orig.get("deduct_day");
            String deductDate    = (String) orig.get("deduct_date");
            String origDeductDate = deductDate;
            String resultCode    = (String) orig.get("result_code");
            String deductType    = (String) orig.get("deduct_type");

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
            pIns.addValue("sendDate", calcSendDate(deductDate, deductType != null ? deductType : "EB"));

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

    /** 단건 수동등록용 billing_seq 생성 */
    private String generateBillingSeq(String spjangcd, String billingYm) {
        int seq = getNextBillingSeqNo(spjangcd, billingYm);
        return billingYm + "-" + String.format("%04d", seq);
    }

    /** 수납내역 조회 (기간별, EB+EC 통합) */
    // 1️⃣ 일반 수납내역 조회 (SUCCESS, FAIL만)
    public List<Map<String, Object>> getBillingHistoryList(
            String startDate, String endDate, String billingType,
            String status, String memberName) {

        return getBillingHistoryListInternal(
                startDate, endDate, billingType, status, memberName,
                false, "SUCCESS,FAIL"  // ✅ 기본적으로 SUCCESS, FAIL만
        );
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
                    AND rb.member_id = b.member_id
                    AND rb.memo LIKE '%불능 / 재청구%'
                    AND rb.status NOT IN ('CANCEL', 'FAIL', 'ERROR')
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
                        AND rb.member_id = b.member_id
                        AND rb.memo LIKE '%불능 / 재청구%'
                        AND rb.status NOT IN ('CANCEL', 'FAIL', 'ERROR')
                        AND rb.deduct_date > b.deduct_date
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

    private String calcSendDate(String deductDate, String deductType) {
        if ("EC".equals(deductType)) return deductDate;
        return cmsHolidayService.getPrevBusinessDay(
                LocalDate.parse(deductDate, DateTimeFormatter.ofPattern("yyyyMMdd"))
                        .minusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMdd")));
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
}
