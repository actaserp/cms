package mes.app.cms.service;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import mes.app.common.TenantContext;
import mes.app.files.NcpObjectStorageService;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
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

        // ★ DISTINCT ON (member_id, apply_type) 제거. (2026-07-15)
        //   계좌변경은 같은 member_id 에 해지행('3')+신규행('1')이 같은 시각에 생기는 세트인데,
        //   DISTINCT ON 이 apply_type 그룹마다 1행만 남기면서 과거 이력행과 경쟁 →
        //   _created 가 동일하면 어느 행이 살아남을지 불확정 → 해지행에 엉뚱한 계좌가 표시됨.
        //
        // ★ 계좌변경 세트 병합:
        //   담당자에게 '변경(해지)/변경(신규)' 두 줄은 내부 구현이 새어나온 것. 업무상 계좌변경은 1건.
        //   → 신규행('1')을 대표로 삼고 해지행('3')은 목록에서 감춘다. 해지행의 계좌는 old_* 로 붙여
        //     "구계좌 → 신계좌" 를 한 줄에 보여준다. pair_id 로 신청/취소 시 두 행을 함께 처리.
        //
        //   단, 두 행의 status 가 서로 다르면 감추지 않는다.
        //   (예: 해지 승인 + 신규 거절 → 그건 더 이상 '변경'이 아니라 '해지 완료 + 신규 재신청 필요' 상태.
        //    한 줄로 뭉치면 구계좌가 이미 죽은 사실이 화면에서 사라져 위험함.)
        //   병합은 반드시 백엔드에서. 프론트 그룹핑은 페이징으로 세트가 갈리면 깨진다.
        String innerSql =
                // ★ 납부자번호는 신청행(r.member_no) 기준. 회원(m.member_no)은 계좌변경 때마다
                //   새 번호로 덮이므로, 화면과 EB13/EI13 파일이 서로 다른 번호를 보게 된다.
                //   신청은 "그때 그 번호로 보낸 것"이 고정돼야 한다. (옛 데이터 대비 m 으로 폴백)
                "SELECT r.id, r.member_id, m.member_name, r.id_number," +
                        "       COALESCE(NULLIF(r.member_no,''), m.member_no) AS member_no, bc.bank_name," +
                        "       r.bank_code, r.bank_account, r.apply_date, r.apply_type, r.change_flag," +
                        "       r.ei13_status, r.ei13_sent_at, r.eb13_status, r.eb13_sent_at," +
                        "       r.eb14_result, r.eb14_fail_code, r.eb14_received_at," +
                        "       r.status, r.memo, r._created, r.agree_file_path," +
                        // 취소 가능 여부: 아직 금결원에 안 나간 PENDING 건만.
                        "       CASE WHEN r.status = 'PENDING' AND COALESCE(r.eb13_status,'PENDING') = 'PENDING'" +
                        "            THEN 'Y' ELSE 'N' END AS cancelable," +
                        // 짝(해지행) 정보 — 계좌변경 세트가 같은 상태로 붙어 있을 때만 채워진다
                        "       pair.id           AS pair_id," +
                        "       pair.bank_account AS old_bank_account," +
                        "       pair.bank_code    AS old_bank_code," +
                        "       obc.bank_name     AS old_bank_name" +
                        "  FROM cms_account_register r" +
                        "  JOIN cms_member m ON m.id = r.member_id" +
                        "  LEFT JOIN cms_bank_code bc ON bc.bank_code = r.bank_code" +
                        "  LEFT JOIN LATERAL (" +
                        "       SELECT c.id, c.bank_code, c.bank_account" +
                        "         FROM cms_account_register c" +
                        "        WHERE c.spjangcd    = r.spjangcd" +
                        "          AND c.member_id   = r.member_id" +
                        "          AND c.change_flag = 'Y' AND c.apply_type = '3'" +
                        "          AND c.apply_date  = r.apply_date" +
                        "          AND c.status      = r.status" +
                        "          AND r.change_flag = 'Y' AND r.apply_type = '1'" +
                        "        ORDER BY c.id DESC LIMIT 1" +
                        "  ) pair ON TRUE" +
                        "  LEFT JOIN cms_bank_code obc ON obc.bank_code = pair.bank_code" +
                        "  WHERE r.spjangcd = :spjangcd" + filters +
                        // 신규행과 같은 상태로 짝이 맞는 해지행은 숨긴다(= 신규행 한 줄로 병합됨).
                        // 짝이 없거나 상태가 갈리면 해지행도 자기 줄로 노출된다.
                        "    AND NOT (r.change_flag = 'Y' AND r.apply_type = '3' AND EXISTS (" +
                        "         SELECT 1 FROM cms_account_register n" +
                        "          WHERE n.spjangcd    = r.spjangcd" +
                        "            AND n.member_id   = r.member_id" +
                        "            AND n.change_flag = 'Y' AND n.apply_type = '1'" +
                        "            AND n.apply_date  = r.apply_date" +
                        "            AND n.status      = r.status))" +
                        // 신청일 역순.
                        "  ORDER BY r.apply_date DESC, r.member_id DESC, r.apply_type, r.id DESC";

        Map<String, Object> countRow = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT COUNT(*) AS cnt FROM (" + innerSql + ") sub", param);
        long totalCount = countRow != null ? ((Number) countRow.get("cnt")).longValue() : 0L;

        param.addValue("pgSize",   size);
        param.addValue("pgOffset", (long) page * size);
        List<Map<String, Object>> rows = sqlRunner.getRows(
                innerSql + " LIMIT :pgSize OFFSET :pgOffset", param);

        return Map.of("data", rows, "totalCount", totalCount, "totalAmount", 0L);
    }


    /**
     * 대기(PENDING) 신청 취소 — 행을 삭제하고 cms_member 상태를 원복한다.
     *
     * ★ 취소 가능 조건: status='PENDING' AND eb13_status='PENDING'
     *   eb13_status='SENT' 면 이미 금결원에 신청이 나간 것이라, 우리 DB에서 지워도
     *   은행 쪽 처리는 그대로 진행된다. 그런 건 '취소'가 아니라 거짓말이 되므로 막는다.
     *   (EB14 결과를 받은 뒤 재등록/재신청으로 처리해야 함)
     *
     * ★ 계좌변경(change_flag='Y')은 해지행('3')+신규행('1')이 한 세트라 반드시 함께 삭제한다.
     *   한쪽만 지우면 반쪽짜리 신청이 남아 EB13이 깨진 상태로 나간다.
     *
     * 원복 대상:
     *   - 단순 해지신청 취소  → cms_member.status : PENDING_CANCEL → ACTIVE
     *   - 계좌변경 취소       → cms_member.agree_yn : 'N' → 'Y' + 계좌를 구계좌로 원복
     *     (2026-07-15부터 changeAccount 가 cms_member 계좌를 즉시 새 값으로 바꾸므로,
     *      취소 시 반드시 되돌려야 한다. 구계좌는 해지행(apply_type='3').bank_account 에 보존돼 있음.
     *      이걸 안 하면 취소했는데 계좌만 새 값으로 남아 다음 청구가 0017 로 실패한다.)
     */
    @Transactional
    public Map<String, Object> cancelPending(List<Long> ids, String userId) {
        if (ids == null || ids.isEmpty()) {
            return Map.of("deleted", 0, "failed", 0, "message", "취소할 항목을 선택하세요.");
        }
        String spjangcd = TenantContext.get();

        List<Map<String, Object>> targets = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT id, member_id, member_name, apply_type, change_flag,
                       status, ei13_status, eb13_status
                FROM cms_account_register
                WHERE spjangcd = :spjangcd AND id IN (:ids)
                """,
                new MapSqlParameterSource("spjangcd", spjangcd).addValue("ids", ids));

        int deleted = 0, failed = 0;
        StringBuilder msg = new StringBuilder();
        java.util.Set<Long> handled = new java.util.HashSet<>();

        for (Map<String, Object> t : targets) {
            Long   rid       = ((Number) t.get("id")).longValue();
            Long   memberId  = t.get("member_id") != null ? ((Number) t.get("member_id")).longValue() : null;
            String name      = str(t.get("member_name"));
            String status    = str(t.get("status"));
            String eb13      = str(t.get("eb13_status"));
            String changeFlg = str(t.get("change_flag"));

            if (handled.contains(rid)) continue;   // 세트로 이미 처리됨

            if (!"PENDING".equals(status)) {
                failed++;
                msg.append(name).append(": 대기 상태가 아니라 취소할 수 없습니다(").append(status).append("). ");
                continue;
            }
            if ("SENT".equals(eb13)) {
                failed++;
                msg.append(name).append(": 이미 금결원에 신청이 전송되어 취소할 수 없습니다. ")
                        .append("결과(EB14) 수신 후 재신청으로 처리하세요. ");
                continue;
            }

            boolean isChange = "Y".equals(changeFlg);

            if (isChange) {
                // 계좌변경 세트 전체 삭제 (해지행+신규행). 하나라도 이미 전송됐으면 세트 전체를 막는다.
                Map<String, Object> sentRow = sqlRunner.getRow(/* skip_tenant_check */
                        """
                        SELECT COUNT(*) AS cnt FROM cms_account_register
                        WHERE spjangcd = :spjangcd AND member_id = :memberId
                          AND change_flag = 'Y' AND status = 'PENDING'
                          AND eb13_status = 'SENT'
                        """,
                        new MapSqlParameterSource("spjangcd", spjangcd).addValue("memberId", memberId));
                if (sentRow != null && ((Number) sentRow.get("cnt")).longValue() > 0) {
                    failed++;
                    msg.append(name).append(": 계좌변경 세트 중 일부가 이미 전송되어 취소할 수 없습니다. ");
                    continue;
                }

                // ★ 삭제 전에 구계좌를 확보한다 (해지행에 보존돼 있음). 삭제 후엔 못 읽는다.
                Map<String, Object> oldRow = sqlRunner.getRow(/* skip_tenant_check */
                        """
                        SELECT bank_code, bank_account, account_holder
                        FROM cms_account_register
                        WHERE spjangcd = :spjangcd AND member_id = :memberId
                          AND change_flag = 'Y' AND apply_type = '3' AND status = 'PENDING'
                        ORDER BY id DESC LIMIT 1
                        """,
                        new MapSqlParameterSource("spjangcd", spjangcd).addValue("memberId", memberId));

                List<Map<String, Object>> setRows = sqlRunner.getRows(/* skip_tenant_check */
                        """
                        SELECT id FROM cms_account_register
                        WHERE spjangcd = :spjangcd AND member_id = :memberId
                          AND change_flag = 'Y' AND status = 'PENDING'
                        """,
                        new MapSqlParameterSource("spjangcd", spjangcd).addValue("memberId", memberId));
                for (Map<String, Object> r : setRows) handled.add(((Number) r.get("id")).longValue());

                int n = sqlRunner.execute(/* skip_tenant_check */
                        """
                        DELETE FROM cms_account_register
                        WHERE spjangcd = :spjangcd AND member_id = :memberId
                          AND change_flag = 'Y' AND status = 'PENDING'
                        """,
                        new MapSqlParameterSource("spjangcd", spjangcd).addValue("memberId", memberId));

                // 동의상태 + 계좌 원복 — changeAccount 가 바꿔놓은 것을 되돌림
                if (oldRow != null && StringUtils.hasText(str(oldRow.get("bank_account")))) {
                    sqlRunner.execute(/* skip_tenant_check */
                            """
                            UPDATE cms_member SET
                                bank_code      = :oldBankCode,
                                bank_account   = :oldBankAccount,
                                account_holder = COALESCE(:oldHolder, account_holder),
                                agree_yn       = 'Y',
                                _modifier_id   = :userId,
                                _modified      = NOW()
                            WHERE id = :memberId AND spjangcd = :spjangcd
                            """,
                            new MapSqlParameterSource("memberId", memberId)
                                    .addValue("spjangcd", spjangcd).addValue("userId", userId)
                                    .addValue("oldBankCode",    str(oldRow.get("bank_code")))
                                    .addValue("oldBankAccount", str(oldRow.get("bank_account")))
                                    .addValue("oldHolder",      StringUtils.hasText(str(oldRow.get("account_holder")))
                                            ? str(oldRow.get("account_holder")) : null));
                    log.info("[RegisterCancel] 계좌변경 취소 memberId={} 삭제={}행 계좌 원복 {} agree_yn=Y",
                            memberId, n, str(oldRow.get("bank_account")));
                } else {
                    // 구계좌를 못 찾은 경우 — 계좌는 건드리지 않고 동의상태만 되돌린다.
                    // (계좌를 임의로 비우면 더 나쁘다. 로그를 남겨 수동 확인 유도)
                    sqlRunner.execute(/* skip_tenant_check */
                            """
                            UPDATE cms_member SET
                                agree_yn     = 'Y',
                                _modifier_id = :userId,
                                _modified    = NOW()
                            WHERE id = :memberId AND spjangcd = :spjangcd
                            """,
                            new MapSqlParameterSource("memberId", memberId)
                                    .addValue("spjangcd", spjangcd).addValue("userId", userId));
                    log.warn("[RegisterCancel] 계좌변경 취소 memberId={} — 해지행에서 구계좌를 찾지 못해 "
                            + "계좌는 새 값 그대로 남습니다. 수동 확인 필요.", memberId);
                }

                deleted += n;

            } else {
                handled.add(rid);
                int n = sqlRunner.execute(/* skip_tenant_check */
                        "DELETE FROM cms_account_register WHERE spjangcd = :spjangcd AND id = :id",
                        new MapSqlParameterSource("spjangcd", spjangcd).addValue("id", rid));

                // 단순 해지신청('3') 취소 → 회원을 다시 활성으로
                if ("3".equals(str(t.get("apply_type")))) {
                    sqlRunner.execute(/* skip_tenant_check */
                            """
                            UPDATE cms_member SET
                                status       = 'ACTIVE',
                                _modifier_id = :userId,
                                _modified    = NOW()
                            WHERE id = :memberId AND spjangcd = :spjangcd
                              AND status = 'PENDING_CANCEL'
                            """,
                            new MapSqlParameterSource("memberId", memberId)
                                    .addValue("spjangcd", spjangcd).addValue("userId", userId));
                }
                deleted += n;
                log.info("[RegisterCancel] 신청 취소 id={} memberId={} applyType={}",
                        rid, memberId, str(t.get("apply_type")));
            }
        }

        String message = deleted > 0
                ? "취소 " + deleted + "건" + (failed > 0 ? " / 실패 " + failed + "건 — " + msg : "")
                : (msg.length() > 0 ? msg.toString() : "취소된 건이 없습니다.");

        return Map.of("deleted", deleted, "failed", failed, "message", message);
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
        if (!StringUtils.hasText(str(member.get("id_number")))) {          // 추가
            log.warn("[Register] 식별번호 누락으로 생성 제외 memberId={}", memberId);
            return null;
        }

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

    /**
     * 계좌등록 신청 — EI13 → EB13 자동 순서 처리.
     * 신규 대기건과 실패/거절 건(재신청)을 함께 받아 한 번에 처리한다.
     */
    public Map<String, Object> register(List<Long> ids) {
        String spjangcd = TenantContext.get();

        // ── 재신청 정규화 ────────────────────────────────────────────────
        // 실패(FAILED)·거절(REJECTED) 건을 재전송 가능한 상태로 되돌린다.
        // 별도의 '재신청' 기능 없이 이 메서드 하나로 신규 + 재신청을 함께 처리한다.
        //  · 해지건(apply_type='3')은 EI13 자체가 불필요 → ei13은 SENT로 유지
        //  · EI13은 당일 전송분만 유효(금결원 24시간 규정) → 날짜가 지났으면 재송신하도록 리셋
        // 계류 중인 건(eb13_status='SENT' + EB14 미수신)은 status가 PENDING이라
        // 여기 걸리지 않고, 아래 eb13Needed 조회에서도 제외되므로 별도 가드가 필요 없다.
        sqlRunner.execute(/* skip_tenant_check */
                """
                UPDATE cms_account_register
                SET status       = 'PENDING',
                    eb13_status  = 'PENDING',
                    eb13_sent_at = NULL,
                    -- 이전 EB14 결과는 반드시 함께 비운다.
                    -- eb14_received_at 이 남아 있으면 getFileList()의 미수신 날짜 추출
                    -- (eb14_received_at IS NULL) 에 걸리지 않아, 재전송 후 결과 파일을
                    -- 영영 수신할 수 없게 된다.
                    eb14_result      = NULL,
                    eb14_fail_code   = NULL,
                    eb14_received_at = NULL,
                    memo         = NULLIF(TRIM(BOTH ' ' FROM
                                       CASE WHEN eb14_fail_code IS NULL THEN ''
                                            ELSE '[이전 불능 ' || eb14_fail_code || '] ' END
                                    || COALESCE('[이전] ' || memo, '')), ''),
                    ei13_status  = CASE WHEN apply_type = '3'                  THEN 'SENT'
                                        WHEN ei13_sent_at::date = CURRENT_DATE THEN ei13_status
                                        ELSE 'PENDING' END,
                    ei13_sent_at = CASE WHEN apply_type = '3'                  THEN ei13_sent_at
                                        WHEN ei13_sent_at::date = CURRENT_DATE THEN ei13_sent_at
                                        ELSE NULL END,
                    _modified    = NOW()
                WHERE id IN (:ids)
                  AND spjangcd = :spjangcd
                  AND (
                        -- 실패·거절 건 재신청
                        status IN ('FAILED', 'REJECTED')
                        -- EI13만 나간 채 날짜가 넘어간 건 (전송 중 오류/타임아웃 등으로 남은 행).
                        -- EI13은 당일분만 유효하므로 EI13부터 다시 보내야 한다.
                        -- 해지건은 ei13_sent_at이 NULL이라 이 비교가 UNKNOWN → 자동 제외.
                     OR (ei13_status = 'SENT' AND eb13_status = 'PENDING'
                         AND ei13_sent_at::date <> CURRENT_DATE)
                      )
                """,
                new MapSqlParameterSource("ids", ids).addValue("spjangcd", spjangcd));

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
                                        .addValue("msg", "금결원 센터오류 (status=" + fileStatus
                                                + ", 동일파일 " + eb13Needed.size() + "건 일괄반려): " + validationMsg));

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


    // ══════════════════════════════════════════════════════════════════
    //  실시간 부가서비스 (금결원 REST API)
    //
    //  파일(EI13/EB13/EB14) 방식과 별개 경로다. 기존 register()/cancelPending()
    //  은 손대지 않았으므로 파일 방식은 그대로 동작한다.
    //
    //  파일 방식과 다른 점:
    //   · 결과가 즉시 온다  → eb14_received_at 을 그 자리에서 채운다.
    //     (안 채우면 CmsEb14ReceiveService.run() 이 오지도 않을 EB14 를 7일간 계속 찾는다)
    //   · 계좌변경은 해지 → 신규 순서를 우리가 지켜야 한다. pair_id 로 세트를 묶고
    //     해지가 APPROVED 된 뒤에만 신규를 열어준다.
    //   · 동의자료는 신규(apply_type='1')만 필요하다. 해지는 바로 해지 API 호출.
    // ══════════════════════════════════════════════════════════════════

    /** EI13 동의자료 구분(숫자) → 실시간 API evidence_file_type(문자열) */
    private static String toEvidenceFileType(String agreeType) {
        if (!StringUtils.hasText(agreeType)) return "PAPER";
        switch (agreeType.trim()) {
            case "1": return "PAPER";                 // 서면
            case "2": return "PUBLIC_SIGNATURE";      // 공동(금융) 전자서명
            case "3": return "GENERAL_SIGNATURE";     // 일반전자서명
            case "4": return "RECORDING";             // 녹취
            case "5": return "ARS";
            case "6": return "ETC";
            default:  return "PAPER";
        }
    }

    /**
     * 요청 추적번호 채번. yyyyMMdd + 4자리 순번.
     * 메모리 카운터는 재시작·다중 인스턴스에서 겹쳐 409(이중요청)를 유발하므로 DB 시퀀스를 쓴다.
     *   CREATE SEQUENCE IF NOT EXISTS cms_realtime_tracking_seq;
     */
    private long nextTrackingNo() {
        // 시퀀스는 사업장과 무관한 전역 채번이므로 테넌트 조건이 없다.
        Map<String, Object> row = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT nextval('cms_realtime_tracking_seq') AS seq /* skip_tenant_check */",
                new MapSqlParameterSource());
        long seq = row != null ? ((Number) row.get("seq")).longValue() : 0L;
        String date = java.time.LocalDate.now()
                .format(java.time.format.DateTimeFormatter.BASIC_ISO_DATE);
        return Long.parseLong(date + String.format("%04d", seq % 10000));
    }

    /** 응답이 성공인지 판정. data.response_code='0000' + TRANSACTION_COMPLETED */
    private boolean isRealtimeOk(JsonNode data) {
        return "0000".equals(data.path("response_code").asText(""))
                && "TRANSACTION_COMPLETED".equals(data.path("realtime_transaction_status").asText(""));
    }

    /**
     * 실시간 신청 모달용 — 선택된 행을 '작업 단위'로 묶어서 돌려준다.
     *
     * 계좌변경 세트는 해지행/신규행 2행이지만 사용자에게는 1건이다.
     * 담당자에게 두 번 선택시키지 않고, 진행 시 해지 → 동의자료 → 계좌등록을
     * 서버가 순서대로 자동 처리한다. 여기서는 그 단계를 미리 보여주기만 한다.
     */
    public List<Map<String, Object>> getRealtimeTargets(List<Long> ids) {
        String spjangcd = TenantContext.get();

        List<Map<String, Object>> rows = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT r.id, r.member_id, r.member_name, r.member_no, r.apply_type,
                       r.bank_code, r.bank_account, r.account_holder, r.id_number,
                       r.agree_type, r.agree_file_path, r.change_flag, r.pair_id,
                       r.ei13_status, r.ei13_sent_at, r.eb13_status, r.status,
                       (r.ei13_sent_at::date = CURRENT_DATE) AS evidence_valid_today,
                       b.bank_name,
                       c.id           AS cancel_id,
                       c.member_no    AS cancel_member_no,
                       c.bank_code    AS cancel_bank_code,
                       c.bank_account AS cancel_bank_account,
                       c.status       AS cancel_status,
                       cb.bank_name   AS cancel_bank_name
                FROM cms_account_register r
                LEFT JOIN cms_bank_code b ON b.bank_code = r.bank_code
                LEFT JOIN cms_account_register c
                       ON c.pair_id = r.pair_id AND c.apply_type = '3' AND r.apply_type = '1'
                LEFT JOIN cms_bank_code cb ON cb.bank_code = c.bank_code
                WHERE r.spjangcd = :spjangcd
                  AND r.id IN (:ids)
                  -- 세트의 해지행은 신규행에 병합해 보여주므로 단독 노출하지 않는다.
                  AND NOT (r.apply_type = '3' AND r.pair_id IS NOT NULL
                           AND EXISTS (SELECT 1 FROM cms_account_register n
                                        WHERE n.pair_id = r.pair_id AND n.apply_type = '1'))
                ORDER BY r.id
                """,
                new MapSqlParameterSource("ids", ids).addValue("spjangcd", spjangcd));

        List<Map<String, Object>> out = new java.util.ArrayList<>();
        for (Map<String, Object> r : rows) {
            String applyType = str(r.get("apply_type"));
            String status    = str(r.get("status"));
            boolean isSet    = r.get("cancel_id") != null;
            boolean evidenceDone = "SENT".equals(str(r.get("ei13_status")))
                    && Boolean.TRUE.equals(r.get("evidence_valid_today"));

            // 진행 단계 안내 — 이미 끝난 단계는 빼고 보여준다.
            List<String> steps = new java.util.ArrayList<>();
            if (isSet && !"APPROVED".equals(str(r.get("cancel_status")))) steps.add("구계좌 해지");
            if ("1".equals(applyType)) {
                if (!evidenceDone) steps.add("동의자료 제출");
                steps.add("계좌등록");
            } else {
                steps.add("계좌해지");
            }

            boolean selectable = true;
            String  blockReason = "";
            if ("APPROVED".equals(status)) {
                selectable = false;
                blockReason = "이미 완료된 건입니다.";
            } else if ("PENDING".equals(status) && "SENT".equals(str(r.get("eb13_status")))) {
                selectable = false;
                blockReason = "파일(EB13)로 전송되어 결과 대기 중입니다.";
            } else if ("1".equals(applyType) && !evidenceDone
                    && !StringUtils.hasText(str(r.get("agree_file_path")))) {
                selectable = false;
                blockReason = "동의서 파일이 없습니다. 먼저 첨부하세요.";
            }

            Map<String, Object> m = new java.util.HashMap<>(r);
            m.put("job_type",      isSet ? "CHANGE" : ("3".equals(applyType) ? "CANCEL" : "NEW"));
            m.put("job_type_nm",   isSet ? "계좌변경" : ("3".equals(applyType) ? "해지" : "신규"));
            m.put("steps",         steps);
            m.put("evidence_done", evidenceDone);
            m.put("selectable",    selectable);
            m.put("block_reason",  blockReason);
            out.add(m);
        }
        return out;
    }

    /**
     * 실시간 처리 — 작업 단위로 전 단계를 자동 진행한다.
     *
     *   해지    : ACCOUNT_UNREGISTRATION
     *   신규    : EVIDENCE_SUBMISSION → ACCOUNT_REGISTRATION
     *   계좌변경: (구계좌)ACCOUNT_UNREGISTRATION → EVIDENCE_SUBMISSION → ACCOUNT_REGISTRATION
     *
     * 중간 실패 시 그 지점에서 멈춘다. 이미 성공한 단계는 되돌리지 않으며(해지 취소 API가 없음),
     * 재시도하면 남은 단계부터 이어서 진행한다.
     *  · 동의자료는 당일 전송분만 유효 → 같은 날 재시도면 계좌등록부터 시작한다.
     */
    @Transactional
    public Map<String, Object> processRealtime(List<Long> ids, String userId) {
        String spjangcd = TenantContext.get();
        int sent = 0, failed = 0;
        List<String> messages = new java.util.ArrayList<>();
        List<Long> okIds = new java.util.ArrayList<>();

        List<Map<String, Object>> jobs = getRealtimeTargets(ids);

        for (Map<String, Object> job : jobs) {
            if (!Boolean.TRUE.equals(job.get("selectable"))) continue;

            long   id       = ((Number) job.get("id")).longValue();
            String jobType  = str(job.get("job_type"));
            String memberNm = str(job.get("member_name"));

            try {
                // ── 1단계: 구계좌 해지 (계좌변경일 때만) ─────────────────
                if ("CHANGE".equals(jobType)
                        && !"APPROVED".equals(str(job.get("cancel_status")))) {
                    long cancelId = ((Number) job.get("cancel_id")).longValue();
                    long tracking = nextTrackingNo();
                    JsonNode res = cmsTokenService.realtimeAccountUnregistration(
                            spjangcd,
                            str(job.get("cancel_bank_code")),
                            str(job.get("cancel_bank_account")).replaceAll("[^0-9]", ""),
                            str(job.get("id_number")).replaceAll("[^0-9]", ""),
                            str(job.get("cancel_member_no")),
                            tracking);

                    if (!isRealtimeOk(res)) {
                        markRealtimeFail(cancelId, tracking, res, userId);
                        failed++;
                        messages.add(memberNm + " — 구계좌 해지 실패: " + res.path("response_message").asText(""));
                        continue;   // 해지가 안 되면 등록으로 넘어가지 않는다
                    }
                    markRealtimeSuccess(cancelId, tracking, userId);
                    okIds.add(cancelId);
                }

                // ── 단독 해지 ────────────────────────────────────────────
                if ("CANCEL".equals(jobType)) {
                    long tracking = nextTrackingNo();
                    JsonNode res = cmsTokenService.realtimeAccountUnregistration(
                            spjangcd,
                            str(job.get("bank_code")),
                            str(job.get("bank_account")).replaceAll("[^0-9]", ""),
                            str(job.get("id_number")).replaceAll("[^0-9]", ""),
                            str(job.get("member_no")),
                            tracking);
                    if (isRealtimeOk(res)) {
                        markRealtimeSuccess(id, tracking, userId);
                        okIds.add(id); sent++;
                    } else {
                        markRealtimeFail(id, tracking, res, userId);
                        failed++;
                        messages.add(memberNm + " — 해지 실패: " + res.path("response_message").asText(""));
                    }
                    continue;
                }

                // ── 2단계: 동의자료 제출 (당일 제출분이 있으면 건너뜀) ────
                String bankCode = str(job.get("bank_code"));
                String account  = str(job.get("bank_account")).replaceAll("[^0-9]", "");
                String idNo     = str(job.get("id_number")).replaceAll("[^0-9]", "");
                String payerNo  = str(job.get("member_no"));

                if (!Boolean.TRUE.equals(job.get("evidence_done"))) {
                    String filePath = str(job.get("agree_file_path"));
                    byte[] fileBytes;
                    try (var stream = storageService.download(filePath)) {
                        fileBytes = stream.readAllBytes();
                    }
                    String fileName = filePath.substring(filePath.lastIndexOf('/') + 1);

                    long tracking = nextTrackingNo();
                    JsonNode res = cmsTokenService.realtimeEvidenceSubmission(
                            spjangcd, bankCode, account, idNo, payerNo, tracking,
                            toEvidenceFileType(str(job.get("agree_type"))), fileName, fileBytes);

                    if (!isRealtimeOk(res)) {
                        markRealtimeFail(id, tracking, res, userId);
                        failed++;
                        messages.add(memberNm + " — 동의자료 제출 실패: " + res.path("response_message").asText(""));
                        continue;   // 동의자료 없이는 등록 불가
                    }
                    sqlRunner.execute(/* skip_tenant_check */
                            """
                            UPDATE cms_account_register
                            SET ei13_status='SENT', ei13_sent_at=NOW(),
                                send_method='API', _modifier_id=:userId, _modified=NOW()
                            WHERE id=:id
                            """,
                            new MapSqlParameterSource("id", id).addValue("userId", userId));
                }

                // ── 3단계: 계좌등록 ──────────────────────────────────────
                long tracking = nextTrackingNo();
                JsonNode res = cmsTokenService.realtimeAccountRegistration(
                        spjangcd, bankCode, account, idNo, payerNo, tracking);

                if (isRealtimeOk(res)) {
                    markRealtimeSuccess(id, tracking, userId);
                    okIds.add(id); sent++;
                } else {
                    markRealtimeFail(id, tracking, res, userId);
                    failed++;
                    messages.add(memberNm + " — 계좌등록 실패: " + res.path("response_message").asText("")
                            + ("CHANGE".equals(jobType) ? " (구계좌 해지는 완료됨 — 등록만 재시도하세요)" : ""));
                }

            } catch (Exception e) {
                log.error("[CmsRealtime] 처리 실패 id={} : {}", id, e.getMessage(), e);
                markRealtimeFail(id, null, "", e.getMessage(), userId);
                failed++;
                messages.add(memberNm + ": " + e.getMessage());
            }
        }

        if (!okIds.isEmpty()) writeApiFileLog(spjangcd, okIds, userId);

        Map<String, Object> out = new java.util.HashMap<>();
        out.put("sent", sent);
        out.put("failed", failed);
        out.put("message", messages.isEmpty() ? null : String.join("\n", messages));
        return out;
    }

    /** 성공 처리 — 실시간은 결과가 즉시 확정되므로 EB14 수신분까지 함께 채운다. */
    private void markRealtimeSuccess(long id, long trackingNo, String userId) {
        sqlRunner.execute(/* skip_tenant_check */
                """
                UPDATE cms_account_register
                SET send_method      = 'API',
                    tracking_no      = :trackingNo,
                    ei13_status      = 'SENT',
                    ei13_sent_at     = COALESCE(ei13_sent_at, NOW()),
                    eb13_status      = 'SENT',
                    eb13_sent_at     = NOW(),
                    eb14_result      = 'Y',
                    eb14_fail_code   = NULL,
                    -- ★ 즉시 채운다. 비워두면 EB14 수신 스케줄러가 계속 대상으로 물어간다.
                    eb14_received_at = NOW(),
                    status           = 'APPROVED',
                    _modifier_id     = :userId,
                    _modified        = NOW()
                WHERE id = :id
                """,
                new MapSqlParameterSource("id", id)
                        .addValue("trackingNo", String.valueOf(trackingNo))
                        .addValue("userId", userId));

        // 신규(등록) 성공이면 회원 인증완료 처리
        sqlRunner.execute(/* skip_tenant_check */
                """
                UPDATE cms_member m
                SET agree_yn = 'Y', _modified = NOW()
                FROM cms_account_register r
                WHERE r.id = :id AND r.member_id = m.id AND r.apply_type = '1'
                """,
                new MapSqlParameterSource("id", id));
    }

    private void markRealtimeFail(long id, Long trackingNo, JsonNode data, String userId) {
        String code = data != null ? data.path("response_code").asText("") : "";
        String msg  = data != null ? data.path("response_message").asText("") : "";
        markRealtimeFail(id, trackingNo, code, msg, userId);
    }

    private void markRealtimeFail(long id, Long trackingNo, String code, String msg, String userId) {
        String memo = StringUtils.hasText(code) ? ("불능 " + code + " " + msg) : msg;
        sqlRunner.execute(/* skip_tenant_check */
                """
                UPDATE cms_account_register
                SET send_method      = 'API',
                    tracking_no      = :trackingNo,
                    eb13_status      = 'FAILED',
                    eb13_sent_at     = NOW(),
                    eb14_result      = 'N',
                    eb14_fail_code   = NULLIF(:code, ''),
                    eb14_received_at = NOW(),
                    status           = 'REJECTED',
                    memo = LEFT(CONCAT_WS(' / ', NULLIF(memo,''), :memo), 500),
                    _modifier_id     = :userId,
                    _modified        = NOW()
                WHERE id = :id
                """,
                new MapSqlParameterSource("id", id)
                        .addValue("trackingNo", trackingNo != null ? String.valueOf(trackingNo) : null)
                        .addValue("code", code != null ? code : "")
                        .addValue("memo", memo != null ? memo : "")
                        .addValue("userId", userId));
    }

    /** 실시간 호출 묶음을 cms_file 에 1행으로 남긴다(file_path 없음, send_type='API'). */
    private void writeApiFileLog(String spjangcd, List<Long> registerIds, String userId) {
        if (registerIds.isEmpty()) return;
        String now = java.time.LocalDateTime.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        var fp = new MapSqlParameterSource();
        fp.addValue("spjangcd", spjangcd);
        fp.addValue("fileName", "RT" + now);
        fp.addValue("cnt",      registerIds.size());
        fp.addValue("userId",   userId);

        Map<String, Object> fileRow = sqlRunner.getRow(/* skip_tenant_check */
                """
                INSERT INTO cms_file (
                    spjangcd, file_name, file_type, file_path,
                    target_date, billing_count, billing_amount,
                    send_type, send_status, sent_at, receive_status, received_at,
                    _creater_id, _created, _modifier_id, _modified
                ) VALUES (
                    :spjangcd, :fileName, 'EB13', NULL,
                    CURRENT_DATE, :cnt, 0,
                    'API', 'SENT', NOW(), 'RECEIVED', NOW(),
                    :userId, NOW(), :userId, NOW()
                ) RETURNING id
                """, fp);
        long fileId = ((Number) fileRow.get("id")).longValue();

        int seq = 1;
        for (Long rid : registerIds) {
            sqlRunner.execute(/* skip_tenant_check */
                    "INSERT INTO cms_file_register (file_id, register_id, line_seq) VALUES (:f, :r, :s)",
                    new MapSqlParameterSource("f", fileId).addValue("r", rid).addValue("s", seq++));
        }
    }

}