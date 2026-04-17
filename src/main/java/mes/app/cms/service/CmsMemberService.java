package mes.app.cms.service;

import lombok.extern.slf4j.Slf4j;
import mes.app.common.TenantContext;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class CmsMemberService {

    @Autowired
    SqlRunner sqlRunner;

    /** 납부자 목록 조회 */
    public List<Map<String, Object>> getMemberList(String memberName, String memberNo, String status) {
        String spjangcd = TenantContext.get();
        MapSqlParameterSource param = new MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);

        String sql = """
                SELECT m.id
                     , m.member_type
                     , CASE m.member_type WHEN 'C' THEN '법인' WHEN 'S' THEN '개인사업자' ELSE '개인' END AS member_type_name
                     , m.member_name
                     , m.member_no
                     , m.id_number
                     , m.phone
                     , m.email
                     , m.zipcd
                     , m.adresa
                     , m.adresb
                     , m.bank_code
                     , b.bank_name
                     , m.bank_account
                     , m.account_holder
                     , m.deduct_day
                     , m.deduct_amount
                     , m.cycle_type
                     , m.cycle_months
                     , m.start_date
                     , m.end_date
                     , m.agree_yn
                     , m.agree_date
                     , m.agree_method
                     , m.status
                     , m.memo
                     , m._created
                     , m._modified
                FROM cms_member m
                LEFT JOIN cms_bank_code b ON b.bank_code = m.bank_code
                WHERE m.spjangcd = :spjangcd
                """;

        if (StringUtils.hasText(memberName)) {
            sql += " AND m.member_name LIKE '%' || :memberName || '%'";
            param.addValue("memberName", memberName);
        }
        if (StringUtils.hasText(memberNo)) {
            sql += " AND m.member_no LIKE '%' || :memberNo || '%'";
            param.addValue("memberNo", memberNo);
        }
        if (StringUtils.hasText(status)) {
            sql += " AND m.status = :status";
            param.addValue("status", status);
        }

        sql += " ORDER BY m.id DESC";
        return sqlRunner.getRows(sql, param);
    }

    /** 납부자 단건 조회 */
    public Map<String, Object> getMember(Long id) {
        String spjangcd = TenantContext.get();
        MapSqlParameterSource param = new MapSqlParameterSource();
        param.addValue("id", id);
        param.addValue("spjangcd", spjangcd);

        String sql = """
                SELECT m.id
                     , m.member_type
                     , m.member_name
                     , m.member_no
                     , m.id_number
                     , m.phone
                     , m.email
                     , m.zipcd
                     , m.adresa
                     , m.adresb
                     , m.bank_code
                     , b.bank_name
                     , m.bank_account
                     , m.account_holder
                     , m.deduct_day
                     , m.deduct_amount
                     , m.cycle_type
                     , m.cycle_months
                     , m.start_date
                     , m.end_date
                     , m.agree_yn
                     , m.agree_date
                     , m.agree_method
                     , m.status
                     , m.memo
                FROM cms_member m
                LEFT JOIN cms_bank_code b ON b.bank_code = m.bank_code
                WHERE m.id = :id AND m.spjangcd = :spjangcd
                """;
        return sqlRunner.getRow(sql, param);
    }

    /** 납부자 저장 (신규/수정) */
    public Long saveMember(Long id, String memberType, String memberName, String memberNo,
                           String idNumber, String phone, String email,
                           String zipcd, String adresa, String adresb,
                           String bankCode, String bankAccount, String accountHolder,
                           String deductDay, Long deductAmount,
                           String cycleType, String cycleMonths,
                           String startDate, String endDate,
                           String agreeYn, String agreeMethod,
                           String status, String memo,
                           String userId) {

        String spjangcd = TenantContext.get();
        MapSqlParameterSource param = new MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);
        param.addValue("memberType", memberType != null ? memberType : "P");
        param.addValue("memberName", memberName);
        param.addValue("memberNo", memberNo);
        param.addValue("idNumber", idNumber);
        param.addValue("phone", phone);
        param.addValue("email", email);
        param.addValue("zipcd", zipcd);
        param.addValue("adresa", adresa);
        param.addValue("adresb", adresb);
        param.addValue("bankCode", bankCode);
        param.addValue("bankAccount", bankAccount);
        param.addValue("accountHolder", accountHolder);
        param.addValue("deductDay", deductDay);
        param.addValue("deductAmount", deductAmount);
        param.addValue("cycleType", cycleType != null ? cycleType : "REGULAR");
        // 비정기일 때 cycle_months 는 NULL
        param.addValue("cycleMonths", "IRREGULAR".equals(cycleType) ? null : cycleMonths);
        param.addValue("startDate", startDate);
        param.addValue("endDate", endDate != null ? endDate : "99991231");
        param.addValue("agreeYn", agreeYn != null ? agreeYn : "N");
        param.addValue("agreeMethod", agreeMethod);
        param.addValue("status", status != null ? status : "ACTIVE");
        param.addValue("memo", memo);
        param.addValue("userId", userId);

        if (id == null) {
            String sql = """
                    INSERT INTO cms_member (
                        spjangcd, member_type, member_name, member_no, id_number,
                        phone, email, zipcd, adresa, adresb,
                        bank_code, bank_account, account_holder,
                        deduct_day, deduct_amount, cycle_type, cycle_months, start_date, end_date,
                        agree_yn, agree_date, agree_method,
                        status, memo,
                        _creater_id, _created, _modifier_id, _modified
                    ) VALUES (
                        :spjangcd, :memberType, :memberName, :memberNo, :idNumber,
                        :phone, :email, :zipcd, :adresa, :adresb,
                        :bankCode, :bankAccount, :accountHolder,
                        :deductDay, :deductAmount, :cycleType, :cycleMonths, :startDate, :endDate,
                        :agreeYn, CASE WHEN :agreeYn = 'Y' THEN NOW() ELSE NULL END, :agreeMethod,
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
                    UPDATE cms_member SET
                        member_type    = :memberType,
                        member_name    = :memberName,
                        member_no      = :memberNo,
                        id_number      = :idNumber,
                        phone          = :phone,
                        email          = :email,
                        zipcd          = :zipcd,
                        adresa         = :adresa,
                        adresb         = :adresb,
                        bank_code      = :bankCode,
                        bank_account   = :bankAccount,
                        account_holder = :accountHolder,
                        deduct_day     = :deductDay,
                        deduct_amount  = :deductAmount,
                        cycle_type     = :cycleType,
                        cycle_months   = :cycleMonths,
                        start_date     = :startDate,
                        end_date       = :endDate,
                        agree_yn       = :agreeYn,
                        agree_date     = CASE WHEN :agreeYn = 'Y' AND agree_date IS NULL THEN NOW() ELSE agree_date END,
                        agree_method   = :agreeMethod,
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

    /** 납부자 삭제 (soft delete) */
    public boolean deleteMember(Long id) {
        String spjangcd = TenantContext.get();
        MapSqlParameterSource param = new MapSqlParameterSource();
        param.addValue("id", id);
        param.addValue("spjangcd", spjangcd);

        String sql = """
                UPDATE cms_member SET status = 'INACTIVE', _modified = NOW()
                WHERE id = :id AND spjangcd = :spjangcd
                """;
        return sqlRunner.execute(sql, param) > 0;
    }
}
