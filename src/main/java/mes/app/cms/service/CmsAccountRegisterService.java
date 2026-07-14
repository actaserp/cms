package mes.app.cms.service;

import com.fasterxml.jackson.databind.JsonNode;
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
import java.util.stream.Collectors;

@Slf4j
@Service
public class CmsAccountRegisterService {

    @Autowired SqlRunner sqlRunner;

    @Autowired
    CmsEi13SendService cmsEi13SendService;

    @Autowired
    CmsEb13SendService cmsEb13SendService;

    @Autowired
    CmsTokenService cmsTokenService;

    @Autowired
    NcpObjectStorageService storageService;

    private String str(Object v) { return v != null ? v.toString() : ""; }

    public Map<String, Object> getList(String memberName, String status, Long memberId, int page, int size) {
        String spjangcd = TenantContext.get();
        var param = new MapSqlParameterSource("spjangcd", spjangcd);

        String filters = "";
        if (StringUtils.hasText(memberName)) {
            filters += " AND m.member_name LIKE '%' || :memberName || '%'";
            param.addValue("memberName", memberName);
        }
        if (StringUtils.hasText(status)) {
            filters += " AND r.status = :status";
            param.addValue("status", status);
        }
        if (memberId != null) {
            filters += " AND r.member_id = :memberId";
            param.addValue("memberId", memberId);
        }

        String innerSql =
                "SELECT DISTINCT ON (r.member_id, r.apply_type)" +
                        "       r.id, r.member_id, m.member_name, m.member_no, bc.bank_name," +
                        "       r.bank_account, r.apply_date, r.apply_type, r.change_flag," +
                        "       r.ei13_status, r.ei13_sent_at, r.eb13_status, r.eb13_sent_at," +
                        "       r.eb14_result, r.eb14_fail_code, r.eb14_received_at," +
                        "       r.status, r.memo, r._created, r.agree_file_path" +
                        "  FROM cms_account_register r" +
                        "  JOIN cms_member m ON m.id = r.member_id" +
                        "  LEFT JOIN cms_bank_code bc ON bc.bank_code = r.bank_code" +
                        "  WHERE r.spjangcd = :spjangcd" + filters +
                        "  ORDER BY r.member_id, r.apply_type, r._created DESC";

        Map<String, Object> countRow = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT COUNT(*) AS cnt FROM (" + innerSql + ") sub", param);
        long totalCount = countRow != null ? ((Number) countRow.get("cnt")).longValue() : 0L;

        param.addValue("pgSize",   size);
        param.addValue("pgOffset", (long) page * size);
        List<Map<String, Object>> rows = sqlRunner.getRows(
                innerSql + " LIMIT :pgSize OFFSET :pgOffset", param);

        return Map.of("data", rows, "totalCount", totalCount, "totalAmount", 0L);
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

        if (!StringUtils.hasText(str(member.get("bank_account")))) return null;

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
        String applyDate = java.time.LocalDate.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));

        if (!ei13Needed.isEmpty()) {

            Map<String, Object> ei13Result = cmsEi13SendService.send(ei13Needed);

            failed += ei13Result.get("failed") != null ? ((Number)ei13Result.get("failed")).intValue() : 0;
            int ei13Sent = ei13Result.get("sent") != null ? ((Number)ei13Result.get("sent")).intValue() : 0;

            if (ei13Sent == 0) {
                return Map.of("sent", sent, "failed", failed);
            }

            // EI13 금결원 검증 확인
            try {
                JsonNode statusNode = cmsTokenService.getFileStatus(spjangcd, "EI13", applyDate, true);
                int fileStatus = statusNode.path("data").path("file_status").asInt(-1);
                log.info("[Register] EI13 파일상태 spjangcd={} fileStatus={}", spjangcd, fileStatus);
                if (fileStatus >= 2 && fileStatus <= 4) {
                    log.warn("[Register] EI13 센터오류 fileStatus={}", fileStatus);
                    return Map.of("sent", 0, "failed", ei13Needed.size());
                }
            } catch (Exception e) {
                log.warn("[Register] EI13 상태확인 실패 (무시): {}", e.getMessage());
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

            // EB13 금결원 검증 확인
            if (sent > 0) {
                try {
                    JsonNode statusNode = cmsTokenService.getFileStatus(spjangcd, "EB13", applyDate, true);
                    int fileStatus = statusNode.path("data").path("file_status").asInt(-1);
                    log.info("[Register] EB13 파일상태 spjangcd={} fileStatus={}", spjangcd, fileStatus);
                    if (fileStatus >= 2 && fileStatus <= 4) {
                        String validationMsg = statusNode.path("data").path("validation_message").asText("");
                        log.warn("[Register] EB13 센터오류 fileStatus={} msg={}", fileStatus, validationMsg);

                        // cms_account_register eb13_status FAILED
                        sqlRunner.execute(/* skip_tenant_check */
                                """
                                UPDATE cms_account_register
                                SET eb13_status='FAILED', status='FAILED',
                                    memo=:msg, _modified=NOW()
                                WHERE id IN (:ids) AND spjangcd=:spjangcd
                                """,
                                new MapSqlParameterSource("ids", eb13Needed)
                                        .addValue("spjangcd", spjangcd)
                                        .addValue("msg", "금결원 센터오류 (status=" + fileStatus + "): " + validationMsg));

                        sent = 0;
                        failed += eb13Needed.size();
                    }
                } catch (Exception e) {
                    log.warn("[Register] EB13 상태확인 실패 (무시): {}", e.getMessage());
                }
            }
        }

        return Map.of("sent", sent, "failed", failed);
    }

    /** 재신청 — REJECTED 건 새 PENDING으로 INSERT */
    public Map<String, Object> reRegister(List<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            return Map.of("sent", 0, "failed", 0);
        }
        String spjangcd = TenantContext.get();

        // 수정 — 조건 제거하고 ids로만 조회
        List<Map<String, Object>> targets = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT id, member_id, member_name, member_no, bank_code, bank_account,
                       account_holder, id_number, member_type, agree_type, agree_ext,
                       agree_file_path, ei13_sent_at, ei13_status, eb13_status, status, apply_type
                FROM cms_account_register
                WHERE id IN (:ids) AND spjangcd = :spjangcd
                """,
                new MapSqlParameterSource("ids", ids).addValue("spjangcd", spjangcd));

        List<Long> registerIds = new java.util.ArrayList<>();

        for (Map<String, Object> t : targets) {
            Long existingId = ((Number) t.get("id")).longValue();
            String ei13Status = str(t.get("ei13_status"));
            String eb13Status = str(t.get("eb13_status"));
            String applyType  = str(t.get("apply_type"));

            // 해지건(apply_type='3')은 EI13 불필요 → eb13/status만 리셋, ei13은 SENT로 유지해 EB13 대상이 되게 함
            if ("3".equals(applyType)) {
                sqlRunner.execute(/* skip_tenant_check */
                        """
                        UPDATE cms_account_register
                        SET eb13_status='PENDING', status='PENDING',
                            eb13_sent_at=NULL, memo=NULL, _modified=NOW()
                        WHERE id = :id
                        """,
                        new MapSqlParameterSource("id", existingId));
                registerIds.add(existingId);
                continue;
            }

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
                    ei13Valid = ei13SentAt.toLocalDateTime().toLocalDate()
                            .isEqual(java.time.LocalDate.now());
                }

                if (ei13Valid) {
                    sqlRunner.execute(/* skip_tenant_check */
                            """
                            UPDATE cms_account_register
                            SET eb13_status='PENDING', status='PENDING',
                                memo=NULL, _modified=NOW()
                            WHERE id = :id
                            """,
                            new MapSqlParameterSource("id", existingId));
                } else {
                    // 날짜 변경 → 전체 리셋
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
            }  else {
                // 추가 — 취소 등 나머지 케이스 → 전체 리셋
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

    public Map<String, Object> createFromErpMembers(String userId) {
        String spjangcd = TenantContext.get();
        int created = 0;

        // agree_yn = 'N'이고 아직 register가 없는 멤버만
        List<Map<String, Object>> members = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT m.id
                FROM cms_member m
                WHERE m.spjangcd = :spjangcd
                AND m.agree_yn = 'N'
                AND m.status = 'ACTIVE'
                AND m.cltcd IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM cms_account_register r
                    WHERE r.member_id = m.id
                    AND r.spjangcd = m.spjangcd
                )
                """,
                new MapSqlParameterSource("spjangcd", spjangcd));

        for (Map<String, Object> m : members) {
            Long memberId = ((Number) m.get("id")).longValue();
            save(memberId, "1", null, null, userId);
            created++;
        }

        log.info("[ERP미인증등록] spjangcd={} 생성={}", spjangcd, created);
        return Map.of("created", created);
    }

    // ===== EB11 (은행접수 해지내역) 수신 =====

    /** TB_FILEINFO(checkseq="EB11", bbsseq)에 적재된 파일 바이트 로드 */
    private byte[] loadEb11Bytes(Integer bbsseq) {
        String spjangcd = TenantContext.get();
        String checkseq = NcpObjectStorageService.toCheckseq("EB11");
        Map<String,Object> fi = sqlRunner.getRow("""
        SELECT filepath, filesvnm FROM TB_FILEINFO
        WHERE checkseq = :checkseq AND bbsseq = :bbsseq AND spjangcd = :spjangcd
        ORDER BY fileseq DESC LIMIT 1
        """,
                new MapSqlParameterSource("checkseq", checkseq)
                        .addValue("bbsseq", bbsseq).addValue("spjangcd", spjangcd));
        if (fi == null) throw new IllegalStateException("EB11 파일을 찾을 수 없습니다.");
        String objectKey = fi.get("filepath") + "/" + fi.get("filesvnm");
        try (var s = storageService.download(objectKey)) {
            return s.readAllBytes();
        } catch (Exception e) {
            throw new IllegalStateException("EB11 파일 로드 실패: " + e.getMessage());
        }
    }

    /** EB11 Data Record(120byte, EUC-KR) 파싱 — 해지대상(신청구분 3/7 또는 CNCL)만 */
    private List<Map<String,Object>> parseEb11(byte[] data) {
        List<Map<String,Object>> out = new java.util.ArrayList<>();
        final int LEN = 120;
        for (int off = 0; off + LEN <= data.length; off += LEN) {
            if (data[off] != 'R') continue;                 // Data Record만
            String d;
            try { d = new String(data, off, LEN, "EUC-KR"); } catch (Exception e) { continue; }
            String applyGb  = d.substring(25, 26).trim();   // 1신규/3해지/7임의해지
            String memberNo = d.substring(26, 46).trim();
            String account  = d.substring(53, 69).trim().replace("-", "");
            String branch   = d.substring(85, 89).trim();   // CHNG/CNCL/숫자

            // 해지대상만: 3·7 또는 CNCL. 신규(1)·계좌변경(CHNG) 제외
            if (!(applyGb.equals("3") || applyGb.equals("7") || branch.equals("CNCL"))) continue;

            Map<String,Object> r = new java.util.LinkedHashMap<>();
            r.put("apply_gb", applyGb);
            r.put("member_no", memberNo);   // 문자열 그대로 (앞자리 0 보존)
            r.put("account", account);
            r.put("branch", branch);
            out.add(r);
        }
        return out;
    }

    /** EB11 미리보기 — 파싱 + cms_member 매칭 (DB 반영 없음) */
    public Map<String,Object> previewEb11(Integer bbsseq) {
        String spjangcd = TenantContext.get();
        List<Map<String,Object>> recs = parseEb11(loadEb11Bytes(bbsseq));

        List<Map<String,Object>> rows = new java.util.ArrayList<>();
        int cancelable = 0, blocked = 0;
        for (Map<String,Object> rec : recs) {
            String memberNo = str(rec.get("member_no"));
            Map<String,Object> m = sqlRunner.getRow("""
            SELECT id, member_name, bank_account, status
            FROM cms_member WHERE spjangcd = :spjangcd AND member_no = :memberNo
            """,   // 숫자변환 금지, 문자열 그대로 매칭
                    new MapSqlParameterSource("spjangcd", spjangcd).addValue("memberNo", memberNo));

            Map<String,Object> row = new java.util.LinkedHashMap<>();
            row.put("member_no", memberNo);
            row.put("file_account", rec.get("account"));
            row.put("apply_gb", rec.get("apply_gb"));

            if (m == null) {
                row.put("member_id", null); row.put("member_name", "(미등록)");
                row.put("checkable", false); row.put("reason", "원장에 없는 납부자번호");
                blocked++;
            } else if ("INACTIVE".equals(str(m.get("status")))) {
                row.put("member_id", m.get("id")); row.put("member_name", m.get("member_name"));
                row.put("db_account", m.get("bank_account"));
                row.put("checkable", false); row.put("reason", "이미 해지됨");
                blocked++;
            } else {
                String fileAcc = str(rec.get("account"));
                String dbAcc   = str(m.get("bank_account")).replace("-", "");
                boolean accOk  = fileAcc.isEmpty() || dbAcc.isEmpty() || dbAcc.equals(fileAcc);
                row.put("member_id", m.get("id")); row.put("member_name", m.get("member_name"));
                row.put("db_account", m.get("bank_account"));
                row.put("checkable", true);
                row.put("reason", accOk ? "" : "계좌 불일치(확인요망)");
                cancelable++;
            }
            rows.add(row);
        }
        Map<String,Object> res = new java.util.LinkedHashMap<>();
        res.put("rows", rows);
        res.put("cancelable", cancelable);
        res.put("blocked", blocked);
        res.put("total", recs.size());
        return res;
    }

    /** EB11 처리 — 선택 회원 해지 + register 이력 + 파일 연결 */
    public Map<String,Object> applyEb11(Integer bbsseq, List<Long> memberIds, String userId) {
        String spjangcd = TenantContext.get();
        if (memberIds == null || memberIds.isEmpty())
            return Map.of("processed", 0, "message", "처리할 대상이 없습니다.");

        String today = java.time.LocalDate.now().toString();
        int processed = 0;
        for (Long memberId : memberIds) {
            // 1) 회원 해지 (status/agree_yn/memo)
            sqlRunner.execute("""
            UPDATE cms_member
            SET status = 'INACTIVE', agree_yn = 'N',
                memo = COALESCE(memo,'') || :tag,
                _modifier_id = :userId, _modified = NOW()
            WHERE id = :memberId AND spjangcd = :spjangcd
            """,
                    new MapSqlParameterSource("memberId", memberId).addValue("spjangcd", spjangcd)
                            .addValue("tag", "\n[EB11 해지 " + today + "]").addValue("userId", userId));

            // 2) register 이력 row
            Long registerId = insertEb11Register(memberId, userId);
            // 3) EB11 파일 연결
            attachEb11File(registerId, bbsseq);
            processed++;
        }
        return Map.of("processed", processed, "message", processed + "명 해지(비활성) 처리됨");
    }

    /** register 이력 INSERT — memo/apply_type 컬럼 없으면 해당 라인 제거 */
    private Long insertEb11Register(Long memberId, String userId) {
        String spjangcd = TenantContext.get();
        Map<String,Object> row = sqlRunner.getRow("""
        INSERT INTO cms_account_register (
            spjangcd, member_id, member_name, member_no,
            bank_code, bank_account, account_holder, id_number, member_type,
            agree_type, apply_type, memo,
            ei13_status, eb13_status, status,
            _creater_id, _created, _modifier_id, _modified
        )
        SELECT :spjangcd, id, member_name, member_no,
               bank_code, bank_account, account_holder, id_number, member_type,
               '3', '3', 'EB11 해지',
               'DONE', 'DONE', 'CANCELED',
               :userId, NOW(), :userId, NOW()
        FROM cms_member WHERE id = :memberId
        RETURNING id
        """,
                new MapSqlParameterSource("spjangcd", spjangcd)
                        .addValue("memberId", memberId).addValue("userId", userId));
        return row != null ? ((Number) row.get("id")).longValue() : null;
    }

    /** EB11 파일을 register row에 연결 */
    private void attachEb11File(Long registerId, Integer bbsseq) {
        if (registerId == null) return;
        String spjangcd = TenantContext.get();
        String checkseq = NcpObjectStorageService.toCheckseq("EB11");
        Map<String,Object> fi = sqlRunner.getRow("""
        SELECT filepath, filesvnm, fileextns FROM TB_FILEINFO
        WHERE checkseq = :checkseq AND bbsseq = :bbsseq AND spjangcd = :spjangcd
        ORDER BY fileseq DESC LIMIT 1
        """,
                new MapSqlParameterSource("checkseq", checkseq)
                        .addValue("bbsseq", bbsseq).addValue("spjangcd", spjangcd));
        if (fi == null) return;
        sqlRunner.execute("""
        UPDATE cms_account_register
        SET agree_file_path = :path, agree_ext = :ext, _modified = NOW()
        WHERE id = :id AND spjangcd = :spjangcd
        """,
                new MapSqlParameterSource("id", registerId).addValue("spjangcd", spjangcd)
                        .addValue("path", fi.get("filepath") + "/" + fi.get("filesvnm"))
                        .addValue("ext", str(fi.get("fileextns")).toLowerCase()));
    }

}