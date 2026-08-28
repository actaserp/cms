package mes.app.cms.service;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mes.domain.services.SqlRunner;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class CmsErpResultSyncService {

    private final SqlRunner sqlRunner;

    /** MS(ERP)의 spjangcd 기본값. tb_xa012_erp.ms_spjangcd가 비어 있을 때만 사용. */
    private static final String MS_SPJANGCD_DEFAULT = "ZZ";

    /**
     * MSSQL 동기화 항목 DTO
     */
    @Data
    public static class SyncItem {
        private final String  memberNo;
        private final String  memberName;
        private final String  bankAccount;
        private final int     lineSeq;
        private final boolean success;
        private final String  disableCd;
        private final long    billingAmount;
        private final long    suAmt;         // fee_request + fee_success
        private final String  cltcd;
    }

    /**
     * 여러 건 일괄 동기화 (PostgreSQL 커밋 완료 후 호출)
     * - 멱등(idempotent): bnkcode가 이미 있으면 건너뜀 → 재실행/백필해도 중복 안 생김
     * - 건별 격리: 한 건이 실패해도 전체 롤백 없이 나머지는 계속 진행
     * - MSSQL은 별도 커넥션. PG는 이미 커밋된 상태이므로 여기 실패는 로그로 표면화.
     *
     * @return {inserted, skipped(이미존재), failed, total}
     */
    public Map<String, Object> syncResults(String spjangcd, String targetDate, List<SyncItem> items) {
        int inserted = 0, skipped = 0, failed = 0, successTargets = 0;
        if (items == null || items.isEmpty()) {
            return summary(0, 0, 0, 0);
        }

        Map<String, Object> erp = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT host, port, db_name, username, password, ms_spjangcd, custcd FROM tb_xa012_erp WHERE spjangcd = :spjangcd",
                new MapSqlParameterSource("spjangcd", spjangcd));
        if (erp == null) {
            log.info("[CmsErpSync] ERP 미연동 스킵 spjangcd={}", spjangcd);
            return summary(0, 0, 0, 0);
        }

        String custcd     = str(erp.get("custcd"));
        // 테이블의 ms_spjangcd 우선, 비어 있으면 ZZ 폴백 (HA 등 잘못된 값 방지)
        String msSpjangcd = str(erp.get("ms_spjangcd"));
        if (msSpjangcd.isBlank()) msSpjangcd = MS_SPJANGCD_DEFAULT;
        String reqDate    = targetDate.substring(2); // YYYYMMDD → YYMMDD
        String tranDate   = "20" + reqDate;          // → YYYYMMDD

        String url = String.format("jdbc:sqlserver://%s:%s;databaseName=%s;encrypt=false",
                str(erp.get("host")), str(erp.get("port")), str(erp.get("db_name")));

        try { Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver"); }
        catch (ClassNotFoundException e) { throw new IllegalStateException("MSSQL 드라이버 없음"); }

        // 같은 납부자·같은 출금일 건이 이미 있는지 (bnkcode 가 아니라 실제 거래 기준으로 판단)
        String existSql = "SELECT COUNT(*) FROM TB_BANK_CMSSAVE WITH(NOLOCK) "
                + "WHERE custcd = ? AND spjangcd = ? AND tran_date = ? AND cmsnum = ?";

        // 그날 이미 쓰인 bnkcode 의 마지막 일련번호(뒤 4자리) — 다음 번호부터 채번한다.
        String maxSeqSql = "SELECT MAX(CAST(RIGHT(bnkcode, 4) AS INT)) "
                + "FROM TB_BANK_CMSSAVE WITH(NOLOCK) "
                + "WHERE custcd = ? AND spjangcd = ? AND tran_date = ?";
        String insertSql = """
                INSERT INTO TB_BANK_CMSSAVE (
                    custcd, spjangcd, bnkcode, cmsnum, bank_tran_id,
                    tran_date, print_content, tran_amt,
                    inout_type, flag, su_amt, cltcd
                ) VALUES (?,?,?,?,?,?,?,?,0,0,?,?)
                """;

        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(
                url, str(erp.get("username")), str(erp.get("password")))) {

            // 건별 즉시 확정(autocommit) → 한 건 실패가 나머지를 롤백하지 않음
            conn.setAutoCommit(true);

            // ★ bnkcode 채번을 line_seq 에 의존하지 않는다.
            //   line_seq 는 '파일 안에서의 순번'이라 같은 출금일에 파일이 둘 이상이면
            //   (EB21 + EC21, 또는 EB21 재생성) 서로 다른 납부자가 같은 bnkcode 를 갖게 되고,
            //   먼저 들어간 건 때문에 뒤의 건이 '이미 존재'로 조용히 스킵된다.
            //   (2026-08-27 신흥빌리지(EC) seq1 이 자리를 차지해 궁전아파트(EB) seq1 이 누락)
            //   → 그날 MSSQL 에 실제로 들어간 마지막 번호 다음부터 이어서 채번한다.
            int nextSeq = 0;
            try (java.sql.PreparedStatement mx = conn.prepareStatement(maxSeqSql)) {
                mx.setString(1, custcd);
                mx.setString(2, msSpjangcd);
                mx.setString(3, tranDate);
                try (java.sql.ResultSet rs = mx.executeQuery()) {
                    if (rs.next()) nextSeq = rs.getInt(1); // 없으면 0
                }
            }

            for (SyncItem item : items) {
                // TB_BANK_CMSSAVE.cltcd 는 ERP 원장(TB_XCLIENT.cltcd) 기준이지만 필수는 아니다.
                // ERP에서 적요(print_content)로 거래처를 식별하므로, 비어 있어도 INSERT 한다.
                // 누락시키는 것보다 미분류로라도 수납내역을 남기는 편이 안전하다.
                if (item.getCltcd() == null || item.getCltcd().isBlank()) {
                    log.warn("[CmsErpSync] cltcd 없음 - 미분류로 INSERT memberNo={} name={}",
                            item.getMemberNo(), item.getMemberName());
                }
                if (!item.isSuccess()) {
                    log.info("[CmsErpSync] 출금실패 INSERT 스킵 memberNo={} disableCd={}",
                            item.getMemberNo(), item.getDisableCd());
                    continue;
                }
                successTargets++;

                String bnkcode = null;
                try {
                    // 1) 이미 있으면 스킵 (멱등) — 같은 출금일·같은 납부자 기준
                    try (java.sql.PreparedStatement chk = conn.prepareStatement(existSql)) {
                        chk.setString(1, custcd);
                        chk.setString(2, msSpjangcd);
                        chk.setString(3, tranDate);
                        chk.setString(4, item.getMemberNo());
                        try (java.sql.ResultSet rs = chk.executeQuery()) {
                            if (rs.next() && rs.getInt(1) > 0) {
                                skipped++;
                                log.info("[CmsErpSync] 이미 존재 스킵 tranDate={} memberNo={}",
                                        tranDate, item.getMemberNo());
                                continue;
                            }
                        }
                    }

                    // bnkcode = YYMMDD + 그날 일련번호(4자리)  (예: 2607010001)
                    bnkcode = reqDate + String.format("%04d", ++nextSeq);

                    // 2) INSERT
                    try (java.sql.PreparedStatement ps = conn.prepareStatement(insertSql)) {
                        ps.setString(1, custcd);
                        ps.setString(2, msSpjangcd);
                        ps.setString(3, bnkcode);
                        ps.setString(4, item.getMemberNo());
                        ps.setString(5, tranDate + item.getMemberNo());
                        ps.setString(6, tranDate);
                        ps.setString(7, item.getMemberName());
                        ps.setLong(8,   item.getBillingAmount()); // tran_amt = 청구금액
                        ps.setLong(9,   item.getSuAmt());         // su_amt   = fee_request + fee_success
                        ps.setString(10, item.getCltcd());
                        ps.execute();
                    }
                    inserted++;
                    log.info("[CmsErpSync] INSERT 완료 bnkcode={} memberNo={}", bnkcode, item.getMemberNo());

                } catch (Exception e) {
                    failed++;
                    log.error("[CmsErpSync] INSERT 실패 bnkcode={} memberNo={}: {}",
                            bnkcode, item.getMemberNo(), e.getMessage(), e);
                }
            }

        } catch (Exception e) {
            // 연결 자체 실패: PG는 이미 커밋됐으므로 결과조회 재동기화로 복구해야 함
            log.error("[CmsErpSync] MSSQL 연결 실패 spjangcd={} (결과조회에서 재동기화 필요): {}",
                    spjangcd, e.getMessage(), e);
            return summary(inserted, skipped, successTargets - inserted - skipped, successTargets);
        }

        if (failed > 0) {
            log.warn("[CmsErpSync] 동기화 일부 실패 spjangcd={} 대상={} 신규={} 스킵={} 실패={} → 결과조회에서 재동기화 필요",
                    spjangcd, successTargets, inserted, skipped, failed);
        } else {
            log.info("[CmsErpSync] 동기화 완료 spjangcd={} 대상={} 신규={} 스킵={}",
                    spjangcd, successTargets, inserted, skipped);
        }
        return summary(inserted, skipped, failed, successTargets);
    }

    /**
     * 결과조회 화면의 "MS 재동기화(없으면 INSERT)" 버튼용.
     * 특정 전송파일(fileId)의 SUCCESS 청구건을 읽어 SyncItem으로 만들고 멱등 동기화.
     * - 계좌(bank_account)는 실제 전송된 값인 cms_billing 기준 (cms_member가 최신이 아닐 수 있으므로).
     * - member_no/cltcd 식별자는 cms_member에서 보강.
     */
    public Map<String, Object> resyncByFileId(String spjangcd, long fileId, String targetDate) {
        List<Map<String, Object>> rows = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT b.billing_amount, b.fee_request, b.fee_success, b.bank_account,
                       COALESCE(b.member_name, m.member_name) AS member_name,
                       m.member_no, m.cltcd, fb.line_seq
                FROM cms_file_billing fb
                JOIN cms_billing b ON b.id = fb.billing_id
                LEFT JOIN cms_member m ON m.id = b.member_id
                WHERE fb.file_id = :fileId
                  AND b.status = 'SUCCESS'
                ORDER BY fb.line_seq
                """,
                new MapSqlParameterSource("fileId", fileId));

        List<SyncItem> items = new ArrayList<>();
        for (Map<String, Object> r : rows) {
            long amt  = num(r.get("billing_amount"));
            long feeR = num(r.get("fee_request"));
            long feeS = num(r.get("fee_success"));
            int  seq  = (int) num(r.get("line_seq"));
            items.add(new SyncItem(
                    str(r.get("member_no")), str(r.get("member_name")), str(r.get("bank_account")),
                    seq, true, null, amt, feeR + feeS, str(r.get("cltcd"))));
        }

        log.info("[CmsErpSync] 재동기화 요청 spjangcd={} fileId={} 대상={}건", spjangcd, fileId, items.size());
        Map<String, Object> res = syncResults(spjangcd, targetDate, items);
        Map<String, Object> out = new LinkedHashMap<>(res);
        out.put("fileId", fileId);
        return out;
    }

    /**
     * 결과조회 화면 "ERP 반영" 버튼용 — 출금일(deduct_date) 기준.
     * 해당 출금일의 SUCCESS 청구건을 읽어 tb_bank_cmssave에 없으면 INSERT(멱등).
     * cms_billing 기준(실제 전송값). billing이 여러 file_billing에 걸리면 최초 line_seq 1건만 사용.
     */
    public Map<String, Object> resyncByDeductDate(String spjangcd, String deductDate) {
        List<Map<String, Object>> rows = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT b.id AS billing_id, b.billing_amount, b.fee_request, b.fee_success, b.bank_account,
                       COALESCE(b.member_name, m.member_name) AS member_name,
                       m.member_no, m.cltcd, fb.line_seq
                FROM cms_billing b
                JOIN cms_file_billing fb ON fb.billing_id = b.id
                LEFT JOIN cms_member m ON m.id = b.member_id
                WHERE b.spjangcd = :spjangcd
                  AND b.deduct_date = :deductDate
                  AND b.status = 'SUCCESS'
                ORDER BY b.id, fb.line_seq
                """,
                new MapSqlParameterSource("spjangcd", spjangcd).addValue("deductDate", deductDate));

        List<SyncItem> items = new ArrayList<>();
        java.util.Set<Object> seen = new java.util.HashSet<>();
        for (Map<String, Object> r : rows) {
            Object bid = r.get("billing_id");
            if (!seen.add(bid)) continue; // billing당 1건(최초 line_seq)만
            long amt  = num(r.get("billing_amount"));
            long feeR = num(r.get("fee_request"));
            long feeS = num(r.get("fee_success"));
            int  seq  = (int) num(r.get("line_seq"));
            items.add(new SyncItem(
                    str(r.get("member_no")), str(r.get("member_name")), str(r.get("bank_account")),
                    seq, true, null, amt, feeR + feeS, str(r.get("cltcd"))));
        }

        log.info("[CmsErpSync] 출금일 재동기화 spjangcd={} deductDate={} 대상={}건", spjangcd, deductDate, items.size());
        Map<String, Object> res = syncResults(spjangcd, deductDate, items);
        Map<String, Object> out = new LinkedHashMap<>(res);
        out.put("deductDate", deductDate);
        return out;
    }

    private Map<String, Object> summary(int inserted, int skipped, int failed, int total) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("inserted", inserted);
        m.put("skipped", skipped);
        m.put("failed", failed);
        m.put("total", total);
        return m;
    }

    private long num(Object v) { return v instanceof Number ? ((Number) v).longValue() : 0L; }

    private String str(Object v) { return v != null ? v.toString() : ""; }
}