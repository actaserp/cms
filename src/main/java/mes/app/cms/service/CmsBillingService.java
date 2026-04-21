package mes.app.cms.service;

import lombok.extern.slf4j.Slf4j;
import mes.app.common.TenantContext;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class CmsBillingService {

    @Autowired
    SqlRunner sqlRunner;

    /** 청구 목록 조회 */
    public List<Map<String, Object>> getBillingList(String billingYm, String deductDate, String memberName, String status) {
        String spjangcd = TenantContext.get();
        var param = new org.springframework.jdbc.core.namedparam.MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);
        param.addValue("billingYm", billingYm);

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
                 , TO_CHAR(TO_DATE(b.deduct_date, 'YYYYMMDD') - INTERVAL '1 day', 'YYYYMMDD') AS send_date
                 , b.status
                 , b.result_code
                 , b.result_msg
                 , b.result_date
                 , b.memo
                 , b._created
                 , b._modified
            FROM cms_billing b
            LEFT JOIN cms_bank_code bc ON bc.bank_code = b.bank_code
            WHERE b.spjangcd = :spjangcd
              AND b.billing_ym = :billingYm
            """;

        if (StringUtils.hasText(deductDate)) {
            sql += " AND b.deduct_date = :deductDate";
            param.addValue("deductDate", deductDate);
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
                            String status, String memo, String userId) {
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
        param.addValue("userId", userId);

        if (id == null) {
            // 청구번호 채번
            String billingSeq = generateBillingSeq(spjangcd, billingYm);
            param.addValue("billingSeq", billingSeq);

            String sql = """
                    INSERT INTO cms_billing (
                        spjangcd, billing_ym, billing_seq,
                        member_id, member_name, bank_code, bank_account, account_holder,
                        billing_amount, deduct_day, deduct_date,
                        status, memo,
                        _creater_id, _created, _modifier_id, _modified
                    ) VALUES (
                        :spjangcd, :billingYm, :billingSeq,
                        :memberId, :memberName, :bankCode, :bankAccount, :accountHolder,
                        :billingAmount, :deductDay, :deductDate,
                        :status, :memo,
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
    public Map<String, Object> generateBilling(String billingYm, String userId) {
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
                  AND m.cycle_type   = 'REGULAR'
                  AND :monthStr      = ANY(STRING_TO_ARRAY(m.cycle_months, ','))
                  AND NOT EXISTS (
                      SELECT 1 FROM cms_billing b
                      WHERE b.member_id  = m.id
                        AND b.billing_ym = :billingYm
                        AND b.spjangcd   = :spjangcd
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
        for (Map<String, Object> m : members) {
            String deductDay = (String) m.get("deduct_day");
            String deductDate = "99".equals(deductDay) ? lastDay : billingYm + deductDay;

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
            ip.addValue("userId",        userId);

            String insertSql = """
                    INSERT INTO cms_billing (
                        spjangcd, billing_ym, billing_seq,
                        member_id, member_name, bank_code, bank_account, account_holder,
                        billing_amount, deduct_day, deduct_date,
                        status, _creater_id, _created, _modifier_id, _modified
                    ) VALUES (
                        :spjangcd, :billingYm, :billingSeq,
                        :memberId, :memberName, :bankCode, :bankAccount, :accountHolder,
                        :billingAmount, :deductDay, :deductDate,
                        'PENDING', :userId, NOW(), :userId, NOW()
                    )
                    """;
            sqlRunner.execute(insertSql, ip);
            count++;
        }

        Map<String, Object> result = new HashMap<>();
        result.put("count", count);
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

    /**
     * EB파일 전송 후 PENDING → REQUESTED 배치 전환 (스케줄러 전용 — skip_tenant_check)
     * billingIds 전체를 단일 UPDATE로 처리
     */
    public int updateStatusToRequested(List<Long> billingIds, Long ebFileId) {
        if (billingIds == null || billingIds.isEmpty()) return 0;
        var param = new org.springframework.jdbc.core.namedparam.MapSqlParameterSource();
        param.addValue("ids",      billingIds);
        param.addValue("ebFileId", ebFileId);
        return sqlRunner.execute(/* skip_tenant_check */
                """
                UPDATE cms_billing
                SET    status     = 'REQUESTED',
                       eb_file_id = :ebFileId,
                       _modified  = NOW()
                WHERE  id IN (:ids)
                  AND  status = 'PENDING'
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
}
