package mes.app.Scheduler.SchedulerService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mes.domain.services.SqlRunner;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

/**
 * 매월 1일 00:30 실행 — 이번달 출금 대상 납부자 → cms_billing 자동생성
 * 납부자 신규등록 시 즉시 생성도 이 메서드를 재사용
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CmsBillingAutoGenerateService {

    private final SqlRunner sqlRunner;

    /** 스케줄러 진입점 — 이번달 전체 */
    public void run() {
        String billingYm = YearMonth.now().format(DateTimeFormatter.ofPattern("yyyyMM"));
        log.info("[CmsBillingAutoGenerate] 시작 - 청구년월: {}", billingYm);

        List<Map<String, Object>> spjangs = sqlRunner.getRows(/* skip_tenant_check */
                "SELECT DISTINCT spjangcd FROM cms_member WHERE status = 'ACTIVE'",
                new MapSqlParameterSource());

        int total = 0;
        for (Map<String, Object> row : spjangs) {
            String spjangcd = (String) row.get("spjangcd");
            try {
                int cnt = generateForSpjang(spjangcd, billingYm, "SYSTEM");
                total += cnt;
                log.info("[CmsBillingAutoGenerate] spjangcd={} {}건 생성", spjangcd, cnt);
            } catch (Exception e) {
                log.error("[CmsBillingAutoGenerate] 실패 spjangcd={}: {}", spjangcd, e.getMessage(), e);
            }
        }
        log.info("[CmsBillingAutoGenerate] 완료 - 총 {}건", total);
    }

    /**
     * 납부자 신규등록 시 즉시 호출 — 해당 납부자 이번달 청구 생성
     */
    public void generateForNewMember(String spjangcd, long memberId, String userId) {
        String billingYm = YearMonth.now().format(DateTimeFormatter.ofPattern("yyyyMM"));
        generateSingleMember(spjangcd, memberId, billingYm, userId);
    }

    /** 사업장 단위 생성 */
    public int generateForSpjang(String spjangcd, String billingYm, String userId) {
        YearMonth ym = YearMonth.parse(billingYm, DateTimeFormatter.ofPattern("yyyyMM"));
        String firstDay  = ym.atDay(1).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String lastDay   = ym.atEndOfMonth().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String monthStr  = String.valueOf(ym.getMonthValue());

        var param = new MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);
        param.addValue("billingYm", billingYm);
        param.addValue("firstDay",  firstDay);
        param.addValue("lastDay",   lastDay);
        param.addValue("monthStr",  monthStr);

        List<Map<String, Object>> members = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT m.id, m.member_name, m.bank_code, m.bank_account, m.account_holder,
                       m.deduct_amount, m.deduct_day
                FROM cms_member m
                WHERE m.spjangcd    = :spjangcd
                  AND m.status      = 'ACTIVE'
                  AND m.agree_yn    = 'Y'
                  AND m.start_date <= :lastDay
                  AND m.end_date   >= :firstDay
                  AND m.cycle_type  = 'REGULAR'
                  AND :monthStr = ANY(STRING_TO_ARRAY(m.cycle_months, ','))
                  AND NOT EXISTS (
                      SELECT 1 FROM cms_billing b
                      WHERE b.member_id  = m.id
                        AND b.billing_ym = :billingYm
                        AND b.spjangcd   = :spjangcd
                  )
                ORDER BY m.id
                """, param);

        if (members.isEmpty()) return 0;

        int nextSeq = getNextSeqNo(spjangcd, billingYm);
        int count = 0;
        for (Map<String, Object> m : members) {
            insertBilling(spjangcd, billingYm, ym, m, nextSeq++, userId);
            count++;
        }
        return count;
    }

    /** 납부자 1명에 대한 즉시 생성 */
    private void generateSingleMember(String spjangcd, long memberId, String billingYm, String userId) {
        YearMonth ym    = YearMonth.parse(billingYm, DateTimeFormatter.ofPattern("yyyyMM"));
        String lastDay  = ym.atEndOfMonth().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String firstDay = ym.atDay(1).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String monthStr = String.valueOf(ym.getMonthValue());

        var param = new MapSqlParameterSource();
        param.addValue("memberId",  memberId);
        param.addValue("spjangcd",  spjangcd);
        param.addValue("billingYm", billingYm);
        param.addValue("firstDay",  firstDay);
        param.addValue("lastDay",   lastDay);
        param.addValue("monthStr",  monthStr);

        Map<String, Object> m = sqlRunner.getRow(/* skip_tenant_check */
                """
                SELECT id, member_name, bank_code, bank_account, account_holder,
                       deduct_amount, deduct_day
                FROM cms_member
                WHERE id       = :memberId
                  AND spjangcd = :spjangcd
                  AND status   = 'ACTIVE'
                  AND agree_yn = 'Y'
                  AND start_date <= :lastDay
                  AND end_date   >= :firstDay
                  AND cycle_type  = 'REGULAR'
                  AND :monthStr = ANY(STRING_TO_ARRAY(cycle_months, ','))
                  AND NOT EXISTS (
                      SELECT 1 FROM cms_billing b
                      WHERE b.member_id = :memberId AND b.billing_ym = :billingYm AND b.spjangcd = :spjangcd
                  )
                """, param);

        if (m == null) return;
        int nextSeq = getNextSeqNo(spjangcd, billingYm);
        insertBilling(spjangcd, billingYm, ym, m, nextSeq, userId);
        log.info("[CmsBillingAutoGenerate] 즉시생성 spjangcd={} memberId={}", spjangcd, memberId);
    }

    private void insertBilling(String spjangcd, String billingYm, YearMonth ym,
                               Map<String, Object> m, int seq, String userId) {
        String lastDay   = ym.atEndOfMonth().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String deductDay = str(m.get("deduct_day"));
        String deductDate = "99".equals(deductDay) ? lastDay : billingYm + deductDay;
        String billingSeq = billingYm + "-" + String.format("%04d", seq);

        var p = new MapSqlParameterSource();
        p.addValue("spjangcd",      spjangcd);
        p.addValue("billingYm",     billingYm);
        p.addValue("billingSeq",    billingSeq);
        p.addValue("memberId",      ((Number) m.get("id")).longValue());
        p.addValue("memberName",    m.get("member_name"));
        p.addValue("bankCode",      m.get("bank_code"));
        p.addValue("bankAccount",   m.get("bank_account"));
        p.addValue("accountHolder", m.get("account_holder"));
        p.addValue("billingAmount", m.get("deduct_amount"));
        p.addValue("deductDay",     deductDay);
        p.addValue("deductDate",    deductDate);
        p.addValue("userId",        userId);

        sqlRunner.execute(/* skip_tenant_check */
                """
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
                """, p);
    }

    private int getNextSeqNo(String spjangcd, String billingYm) {
        var p = new MapSqlParameterSource();
        p.addValue("spjangcd", spjangcd);
        p.addValue("billingYm", billingYm);
        Map<String, Object> row = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT COALESCE(MAX(CAST(SPLIT_PART(billing_seq,'-',2) AS INT)),0) AS max_seq FROM cms_billing WHERE spjangcd=:spjangcd AND billing_ym=:billingYm",
                p);
        return row != null ? ((Number) row.get("max_seq")).intValue() + 1 : 1;
    }

    private String str(Object v) { return v != null ? v.toString() : ""; }
}
