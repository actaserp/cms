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
                "SELECT r.id, r.member_id, m.member_name, r.id_number, m.member_no, bc.bank_name," +
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

}