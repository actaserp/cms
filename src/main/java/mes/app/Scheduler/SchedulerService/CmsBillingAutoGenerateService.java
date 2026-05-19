package mes.app.Scheduler.SchedulerService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mes.app.cms.service.CmsHolidayService;
import mes.domain.services.SqlRunner;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

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

    private final SqlRunner          sqlRunner;
    private final CmsHolidayService  cmsHolidayService;

    /** 스케줄러 진입점 — 이번달 전체 */
    public void run() {
        String billingYm = YearMonth.now().plusMonths(1).format(DateTimeFormatter.ofPattern("yyyyMM"));
        log.info("[CmsBillingAutoGenerate] 시작 - 청구년월: {}", billingYm);

        // ✨ auto_billing_yn = 'Y' 체크 추가!
        List<Map<String, Object>> spjangs = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT DISTINCT m.spjangcd FROM cms_member m
                JOIN tb_xa012_cms c ON c.spjangcd = m.spjangcd
                WHERE m.status = 'ACTIVE'
                  AND c.auto_billing_yn = 'Y'
                """,
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
                        m.deduct_amount, m.deduct_day,
                        m.pause_start_date, m.pause_end_date
                 FROM cms_member m
                 WHERE m.spjangcd    = :spjangcd
                   AND m.status      = 'ACTIVE'
                   AND m.agree_yn    = 'Y'
                   AND m.start_date <= :lastDay
                   AND m.end_date   >= :firstDay
                   AND (
                       m.cycle_type = 'REGULAR'
                       OR (
                           m.cycle_type = 'IRREGULAR'
                           AND :monthStr = ANY(STRING_TO_ARRAY(m.cycle_months, ','))
                       )
                   )
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
        int skippedByPause = 0;
        for (Map<String, Object> m : members) {
            if (isPausedInBillingMonth(m, ym)) {
                log.info("[CmsBillingAutoGenerate] 중지 기간 → 청구 생성 스킵 - 납부자: {}", m.get("member_name"));
                skippedByPause++;
                continue;
            }

            insertBilling(spjangcd, billingYm, ym, m, nextSeq++, userId);
            count++;
        }

        if (skippedByPause > 0) {
            log.info("[CmsBillingAutoGenerate] spjangcd={} 중지 기간으로 {}건 스킵", spjangcd, skippedByPause);
        }

        return count;
    }

    /**
     * 해당 청구년월이 납부자의 중지 기간과 겹치는지 확인.
     * 청구는 다음달치를 미리 생성하므로 "오늘"이 아니라 "청구 대상 월" 기준으로 판단해야 함.
     * 중지기간이 청구월의 첫날~마지막날과 하루라도 겹치면 해당 월 청구를 스킵.
     */
    private boolean isPausedInBillingMonth(Map<String, Object> member, YearMonth billingYm) {
        String pauseStartStr = objToStr(member.get("pause_start_date"));
        String pauseEndStr   = objToStr(member.get("pause_end_date"));

        if (!StringUtils.hasText(pauseStartStr) || !StringUtils.hasText(pauseEndStr)) {
            return false;
        }

        try {
            // DB가 DATE 타입으로 반환하면 "yyyy-MM-dd", CHAR/VARCHAR면 "yyyyMMdd" 형태일 수 있음
            LocalDate pauseStart = parseFlexDate(pauseStartStr);
            LocalDate pauseEnd   = parseFlexDate(pauseEndStr);

            LocalDate billingFirst = billingYm.atDay(1);
            LocalDate billingLast  = billingYm.atEndOfMonth();

            // 중지기간과 청구월이 하루라도 겹치면 스킵
            // (pauseStart <= billingLast) AND (pauseEnd >= billingFirst)
            return !pauseStart.isAfter(billingLast) && !pauseEnd.isBefore(billingFirst);
        } catch (Exception e) {
            log.error("[CmsBillingAutoGenerate] 중지 기간 파싱 오류 - member: {}, pauseStart: {}, pauseEnd: {}, error: {}",
                    member.get("member_name"), pauseStartStr, pauseEndStr, e.getMessage());
            return false;
        }
    }

    /** DB 반환값이 java.sql.Date, LocalDate, String 어느 타입이든 안전하게 String 변환 */
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
                       deduct_amount, deduct_day,
                       pause_start_date, pause_end_date
                FROM cms_member
                WHERE id       = :memberId
                  AND spjangcd = :spjangcd
                  AND status   = 'ACTIVE'
                  AND agree_yn = 'Y'
                  AND start_date <= :lastDay
                  AND end_date   >= :firstDay
                  AND (
                      cycle_type = 'REGULAR'
                      OR (
                          cycle_type = 'IRREGULAR'
                          AND :monthStr = ANY(STRING_TO_ARRAY(cycle_months, ','))
                      )
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM cms_billing b
                      WHERE b.member_id = :memberId AND b.billing_ym = :billingYm AND b.spjangcd = :spjangcd
                  )
                """, param);

        if (m == null) return;

        // 중지 기간 체크 — 청구년월 기준
        if (isPausedInBillingMonth(m, ym)) {
            log.info("[CmsBillingAutoGenerate] 중지 기간 → 청구 생성 스킵 - 납부자: {}", m.get("member_name"));
            return;
        }

        int nextSeq = getNextSeqNo(spjangcd, billingYm);
        insertBilling(spjangcd, billingYm, ym, m, nextSeq, userId);
        log.info("[CmsBillingAutoGenerate] 즉시생성 spjangcd={} memberId={}", spjangcd, memberId);
    }

    private void insertBilling(String spjangcd, String billingYm, YearMonth ym,
                               Map<String, Object> m, int seq, String userId) {
        String lastDay   = ym.atEndOfMonth().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String deductDay = str(m.get("deduct_day"));
        String deductDate = "99".equals(deductDay) ? lastDay : billingYm + deductDay;

        // 휴일이면 다음 영업일로 보정
        deductDate = cmsHolidayService.getNextBusinessDay(deductDate);

        // 보정된 출금일 기준으로 마감 체크
        // - 오늘 이전 출금일 → 스킵
        // - 보정된 출금일의 D-1(신청 마감일)이 오늘 15시 이후 → 스킵
        String todayStr    = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String tomorrowStr = LocalDate.now().plusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        int nowHour = java.time.LocalTime.now().getHour();
        if (deductDate.compareTo(todayStr) < 0) {
            log.info("[BillingAutoGenerate] 스킵 - 오늘 이전 출금일: {} member={}", deductDate, m.get("member_name"));
            return;
        }
        // 출금일의 전 영업일(신청 마감일)이 오늘인데 15시 이후면 스킵
        String deadlineDay = cmsHolidayService.getPrevBusinessDay(
                LocalDate.parse(deductDate, DateTimeFormatter.ofPattern("yyyyMMdd"))
                        .minusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMdd"))
        );
        if (deadlineDay.equals(todayStr) && nowHour >= 15) {
            log.info("[BillingAutoGenerate] 스킵 - 신청마감(15시) 초과: deductDate={} member={}", deductDate, m.get("member_name"));
            return;
        }

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
        String sendDate = "EB".equals(/* deductType 없으므로 EB 고정 */ "EB")
                ? cmsHolidayService.getPrevBusinessDay(
                LocalDate.parse(deductDate, DateTimeFormatter.ofPattern("yyyyMMdd"))
                        .minusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMdd")))
                : deductDate;
        p.addValue("sendDate", sendDate);

        sqlRunner.execute(/* skip_tenant_check */
                """
                INSERT INTO cms_billing (
                    spjangcd, billing_ym, billing_seq,
                    member_id, member_name, bank_code, bank_account, account_holder,
                    billing_amount, deduct_day, deduct_date, send_date,
                    status, _creater_id, _created, _modifier_id, _modified
                ) VALUES (
                    :spjangcd, :billingYm, :billingSeq,
                    :memberId, :memberName, :bankCode, :bankAccount, :accountHolder,
                    :billingAmount, :deductDay, :deductDate, :sendDate,
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