package mes.app.cms.service;

import lombok.extern.slf4j.Slf4j;
import mes.app.common.TenantContext;
import mes.app.files.NcpObjectStorageService;
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

    @Autowired
    private CmsAccountRegisterService cmsAccountRegisterService;

    @Autowired
    private NcpObjectStorageService storageService;

    private String str(Object v) { return v != null ? v.toString() : ""; }

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
                 , CASE
                       WHEN m.agree_yn = 'Y'        THEN '인증완료'
                       WHEN r.status = 'REJECTED'   THEN '인증거절'
                       WHEN r.status IN ('PENDING')  THEN '인증대기'
                       ELSE '미신청'
                   END AS agree_status
            FROM cms_member m
            LEFT JOIN cms_bank_code b ON b.bank_code = m.bank_code
            LEFT JOIN LATERAL (
                SELECT status FROM cms_account_register
                WHERE member_id = m.id AND spjangcd = m.spjangcd
                ORDER BY _created DESC LIMIT 1
            ) r ON true
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
            // 납부자번호 자동 채번
            Map<String, Object> seqRow = sqlRunner.getRow(
                    """
                    SELECT COALESCE(MAX(CAST(SUBSTRING(member_no, LENGTH(:spjangcd) + 1) AS INTEGER)), 0) + 1 AS next_seq
                    FROM cms_member
                    WHERE spjangcd = :spjangcd
                      AND member_no ~ ('^' || :spjangcd || '[0-9]+$')
                    """,
                    new MapSqlParameterSource("spjangcd", spjangcd));
            int nextSeq = seqRow != null ? ((Number) seqRow.get("next_seq")).intValue() : 1;
            String autoMemberNo = spjangcd + String.format("%06d", nextSeq);
            param.addValue("memberNo", autoMemberNo);

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
            Long savedId = ((Number) row.get("id")).longValue();

            String checkseq = NcpObjectStorageService.toCheckseq("cms_member");
            Map<String, Object> fileInfo = sqlRunner.getRow(
                    """
                    SELECT filepath, filesvnm, fileextns
                    FROM TB_FILEINFO
                    WHERE checkseq = :checkseq
                      AND bbsseq   = :bbsseq
                      AND spjangcd = :spjangcd
                    ORDER BY fileseq DESC
                    LIMIT 1
                    """,
                    new MapSqlParameterSource("checkseq", checkseq)
                            .addValue("bbsseq", savedId.intValue())
                            .addValue("spjangcd", spjangcd));

            String agreeFilePath = null;
            String agreeExt      = null;
            if (fileInfo != null) {
                agreeFilePath = fileInfo.get("filepath") + "/" + fileInfo.get("filesvnm");
                agreeExt      = str(fileInfo.get("fileextns")).toLowerCase();
            }

            // cms_account_register 자동 생성
            cmsAccountRegisterService.save(savedId, "1", agreeExt, agreeFilePath, userId);
            return  savedId;

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

            // cms_account_register 동기화 (PENDING 건만 — 이미 SENT/APPROVED는 건드리지 않음)
            if (affected > 0) {
                sqlRunner.execute(
                        """
                        UPDATE cms_account_register
                        SET member_name    = (SELECT member_name FROM cms_member WHERE id = :memberId),
                            bank_code      = :bankCode,
                            bank_account   = :bankAccount,
                            account_holder = :accountHolder,
                            id_number      = :idNumber,
                            member_type    = :memberType,
                            _modified      = NOW()
                        WHERE member_id = :memberId
                          AND spjangcd  = :spjangcd
                          AND status    = 'PENDING'
                          AND ei13_status IN ('PENDING', 'FAILED')
                        """,
                        new MapSqlParameterSource("memberId", id)
                                .addValue("spjangcd", spjangcd)
                                .addValue("bankCode", bankCode)
                                .addValue("bankAccount", bankAccount)
                                .addValue("accountHolder", accountHolder)
                                .addValue("idNumber", idNumber)
                                .addValue("memberType", memberType));
            }

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
