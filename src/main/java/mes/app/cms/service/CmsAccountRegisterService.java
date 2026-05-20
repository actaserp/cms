package mes.app.cms.service;

import mes.app.common.TenantContext;
import mes.app.files.NcpObjectStorageService;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class CmsAccountRegisterService {

    @Autowired SqlRunner sqlRunner;

    @Autowired
    CmsEi13SendService cmsEi13SendService;

    @Autowired
    CmsEb13SendService cmsEb13SendService;

    private String str(Object v) { return v != null ? v.toString() : ""; }

    public List<Map<String, Object>> getList(String memberName, String status, Long memberId) {
        String spjangcd = TenantContext.get();
        var param = new MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);

        String sql = """
        SELECT DISTINCT ON (r.member_id) 
               r.id,
               r.member_id,
               m.member_name,
               m.member_no,
               bc.bank_name,
               r.bank_account,
               r.apply_date,
               r.apply_type,
               r.ei13_status,
               r.ei13_sent_at,
               r.eb13_status,
               r.eb13_sent_at,
               r.eb14_result,
               r.eb14_fail_code,
               r.eb14_received_at,
               r.status,
               r.memo,
               r._created,
               r.agree_file_path
        FROM cms_account_register r
        JOIN cms_member m ON m.id = r.member_id
        LEFT JOIN cms_bank_code bc ON bc.bank_code = r.bank_code
        WHERE r.spjangcd = :spjangcd
        """;

        if (StringUtils.hasText(memberName)) {
            sql += " AND m.member_name LIKE '%' || :memberName || '%'";
            param.addValue("memberName", memberName);
        }
        if (StringUtils.hasText(status)) {
            sql += " AND r.status = :status";
            param.addValue("status", status);
        }

        if (memberId != null) {
            sql += " AND r.member_id = :memberId";
            param.addValue("memberId", memberId);
        }

        sql += " ORDER BY r.member_id, r._created DESC";
        return sqlRunner.getRows(sql, param);
    }

    public Long save(Long memberId, String agreeType, String agreeExt,
                     String agreeFilePath, String userId) {
        String spjangcd = TenantContext.get();

        // 납부자 정보 조회
        Map<String, Object> member = sqlRunner.getRow(
                """
                SELECT member_no, bank_code, bank_account, account_holder,
                       id_number, member_type
                FROM cms_member WHERE id = :id AND spjangcd = :spjangcd
                """,
                new MapSqlParameterSource("id", memberId).addValue("spjangcd", spjangcd));

        if (member == null) return null;

        var param = new MapSqlParameterSource();
        param.addValue("spjangcd",      spjangcd);
        param.addValue("memberId",      memberId);
        param.addValue("memberNo",      member.get("member_no"));
        param.addValue("bankCode",      member.get("bank_code"));
        param.addValue("bankAccount",   member.get("bank_account"));
        param.addValue("accountHolder", member.get("account_holder"));
        param.addValue("idNumber",      member.get("id_number"));
        param.addValue("memberType",    member.get("member_type"));
        param.addValue("agreeType",     StringUtils.hasText(agreeType) ? agreeType : "1");
        param.addValue("agreeExt",      agreeExt);
        param.addValue("agreeFilePath", agreeFilePath);
        param.addValue("userId",        userId);

        Map<String, Object> row = sqlRunner.getRow(
                """
                INSERT INTO cms_account_register (
                    spjangcd, member_id, member_name, member_no,
                    bank_code, bank_account, account_holder, id_number, member_type,
                    agree_type, agree_ext, agree_file_path,
                    ei13_status, eb13_status, status,
                    _creater_id, _created, _modifier_id, _modified
                )
                SELECT :spjangcd, :memberId, member_name, :memberNo,
                       :bankCode, :bankAccount, :accountHolder, :idNumber, :memberType,
                       :agreeType, :agreeExt, :agreeFilePath,
                       'PENDING', 'PENDING', 'PENDING',
                       :userId, NOW(), :userId, NOW()
                FROM cms_member WHERE id = :memberId
                RETURNING id
                """, param);

        return row != null ? ((Number) row.get("id")).longValue() : null;
    }

    /** EI13 → EB13 자동 순서 처리 */
    public Map<String, Object> register(List<Long> ids) {
        String spjangcd = TenantContext.get();

        List<Long> ei13Needed = sqlRunner.getRows(/* skip_tenant_check */
                        "SELECT id FROM cms_account_register WHERE id IN (:ids) AND ei13_status IN ('PENDING','FAILED') and spjangcd = :spjangcd",
                        new MapSqlParameterSource("ids", ids).addValue("spjangcd", spjangcd))
                .stream().map(r -> ((Number)r.get("id")).longValue()).collect(Collectors.toList());

        int sent = 0, failed = 0;

        if (!ei13Needed.isEmpty()) {
            Map<String, Object> ei13Result = cmsEi13SendService.send(ei13Needed);
            // EI13 실패 건수 반영
            failed += ei13Result.get("failed") != null ? ((Number)ei13Result.get("failed")).intValue() : 0;
            int ei13Sent = ei13Result.get("sent") != null ? ((Number)ei13Result.get("sent")).intValue() : 0;

            if (ei13Sent == 0) {
                // EI13 전부 실패면 EB13 시도 안 함
                return Map.of("sent", sent, "failed", failed);
            }
        }

        List<Long> eb13Needed = sqlRunner.getRows(/* skip_tenant_check */
                        "SELECT id FROM cms_account_register WHERE id IN (:ids) AND ei13_status = 'SENT' AND eb13_status IN ('PENDING','FAILED') and spjangcd = :spjangcd",
                        new MapSqlParameterSource("ids", ids).addValue("spjangcd", spjangcd))
                .stream().map(r -> ((Number)r.get("id")).longValue()).collect(Collectors.toList());

        if (!eb13Needed.isEmpty()) {
            Map<String, Object> eb13Result = cmsEb13SendService.send(eb13Needed);
            sent  += eb13Result.get("sent")   != null ? ((Number)eb13Result.get("sent")).intValue()   : 0;
            failed += eb13Result.get("failed") != null ? ((Number)eb13Result.get("failed")).intValue() : 0;
        }

        return Map.of("sent", sent, "failed", failed);
    }

    /** 재신청 — REJECTED 건 새 PENDING으로 INSERT */
    public Map<String, Object> reRegister(List<Long> ids) {
        String spjangcd = TenantContext.get();

        List<Map<String, Object>> targets = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT id, member_id, member_name, member_no, bank_code, bank_account,
                       account_holder, id_number, member_type, agree_type, agree_ext,
                       agree_file_path, ei13_sent_at, ei13_status, eb13_status, status
                FROM cms_account_register
                WHERE id IN (:ids) AND spjangcd = :spjangcd
                  AND (status = 'REJECTED' OR ei13_status = 'FAILED' OR eb13_status = 'FAILED')
                """,
                new MapSqlParameterSource("ids", ids).addValue("spjangcd", spjangcd));

        List<Long> registerIds = new java.util.ArrayList<>();

        for (Map<String, Object> t : targets) {
            Long existingId = ((Number) t.get("id")).longValue();
            String ei13Status = str(t.get("ei13_status"));
            String eb13Status = str(t.get("eb13_status"));

            if ("FAILED".equals(ei13Status) || "REJECTED".equals(str(t.get("status")))) {
                // EI13 실패 or REJECTED → 전체 리셋
                sqlRunner.execute(/* skip_tenant_check */
                        """
                        UPDATE cms_account_register
                        SET ei13_status='PENDING', eb13_status='PENDING',
                            status='PENDING', memo=NULL,
                            ei13_sent_at=NULL, eb13_sent_at=NULL, _modified=NOW()
                        WHERE id = :id
                        """,
                        new MapSqlParameterSource("id", existingId));

            } else if ("FAILED".equals(eb13Status)) {
                // EB13 실패 → EI13 송신 시각 확인
                Object ei13SentAtObj = t.get("ei13_sent_at");
                boolean ei13Valid = false;
                if (ei13SentAtObj != null) {
                    java.sql.Timestamp ei13SentAt = (java.sql.Timestamp) ei13SentAtObj;
                    ei13Valid = ei13SentAt.toLocalDateTime()
                            .isAfter(java.time.LocalDateTime.now().minusHours(24));
                }

                if (ei13Valid) {
                    // 24시간 이내 → EB13만 리셋
                    sqlRunner.execute(/* skip_tenant_check */
                            """
                            UPDATE cms_account_register
                            SET eb13_status='PENDING', status='PENDING',
                                memo=NULL, _modified=NOW()
                            WHERE id = :id
                            """,
                            new MapSqlParameterSource("id", existingId));
                } else {
                    // 24시간 초과 → 전체 리셋
                    sqlRunner.execute(/* skip_tenant_check */
                            """
                            UPDATE cms_account_register
                            SET ei13_status='PENDING', eb13_status='PENDING',
                                status='PENDING', memo=NULL,
                                ei13_sent_at=NULL, eb13_sent_at=NULL, _modified=NOW()
                            WHERE id = :id
                            """,
                            new MapSqlParameterSource("id", existingId));
                }
            }
            registerIds.add(existingId);
        }

        // 바로 신청까지 처리
        return register(registerIds);
    }

    /** 동의서 파일 첨부/변경 */
    public void updateAgreeFile(Long registerId, Long memberId) {
        String spjangcd = TenantContext.get();

        String checkseq = NcpObjectStorageService.toCheckseq("AGREE");
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
                        .addValue("bbsseq", registerId.intValue())
                        .addValue("spjangcd", spjangcd));

        if (fileInfo == null) return;

        String agreeFilePath = fileInfo.get("filepath") + "/" + fileInfo.get("filesvnm");
        String agreeExt      = str(fileInfo.get("fileextns")).toLowerCase();

        var param = new MapSqlParameterSource();
        param.addValue("id",           registerId);
        param.addValue("spjangcd",     spjangcd);
        param.addValue("agreeFilePath", agreeFilePath);
        param.addValue("agreeExt",     agreeExt);

        sqlRunner.execute(
                """
                UPDATE cms_account_register
                SET agree_file_path = :agreeFilePath,
                    agree_ext       = :agreeExt,
                    _modified       = NOW()
                WHERE id = :id AND spjangcd = :spjangcd
                """, param);

        // cms_member agree_method FILE로 업데이트
        if (memberId != null) {
            sqlRunner.execute(
                    """
                    UPDATE cms_member
                    SET agree_method = 'FILE', _modified = NOW()
                    WHERE id = :memberId AND spjangcd = :spjangcd
                    """,
                    new MapSqlParameterSource("memberId", memberId)
                            .addValue("spjangcd", spjangcd));
        }
    }

    public void clearAgreeFile(Long registerId) {
        String spjangcd = TenantContext.get();
        sqlRunner.execute(
                """
                UPDATE cms_account_register
                SET agree_file_path = NULL,
                    agree_ext       = NULL,
                    _modified       = NOW()
                WHERE id = :id AND spjangcd = :spjangcd
                """,
                new MapSqlParameterSource("id", registerId)
                        .addValue("spjangcd", spjangcd));
    }
}