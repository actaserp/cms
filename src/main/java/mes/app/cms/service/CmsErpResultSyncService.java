package mes.app.cms.service;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mes.domain.services.SqlRunner;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class CmsErpResultSyncService {

    private final SqlRunner sqlRunner;

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
        private final String cltcd;
    }

    /**
     * 여러 건 일괄 동기화 (PostgreSQL 커밋 완료 후 호출)
     * MSSQL은 별도 트랜잭션 - 실패 시 로그만 남김
     */
    public void syncResults(String spjangcd, String targetDate, List<SyncItem> items) {
        if (items == null || items.isEmpty()) return;

        Map<String, Object> erp = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT host, port, db_name, username, password, ms_spjangcd, custcd FROM tb_xa012_erp WHERE spjangcd = :spjangcd",
                new MapSqlParameterSource("spjangcd", spjangcd));
        if (erp == null) return; // ERP 미연동이면 스킵

        String msSpjangcd = str(erp.get("ms_spjangcd"));
        String custcd     = str(erp.get("custcd"));
        String reqDate    = targetDate.substring(2); // YYYYMMDD → YYMMDD
        String tranDate   = "20" + reqDate;

        String url = String.format("jdbc:sqlserver://%s:%s;databaseName=%s;encrypt=false",
                str(erp.get("host")), str(erp.get("port")), str(erp.get("db_name")));

        try { Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver"); }
        catch (ClassNotFoundException e) { throw new IllegalStateException("MSSQL 드라이버 없음"); }

        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(
                url, str(erp.get("username")), str(erp.get("password")))) {

            conn.setAutoCommit(false);
            try {
                for (SyncItem item : items) {
                    if (!item.isSuccess()) {
                        log.info("[CmsErpSync] 출금실패 INSERT 스킵 memberNo={} disableCd={}", item.getMemberNo(), item.getDisableCd());
                        continue;
                    }

                    String bnkcode = reqDate.substring(0, 2) + reqDate.substring(2, 6)
                            + String.format("%04d", item.getLineSeq());

                    try (java.sql.PreparedStatement ps = conn.prepareStatement("""
                        INSERT INTO TB_BANK_CMSSAVE (
                            custcd, spjangcd, bnkcode, cmsnum, bank_tran_id,
                            tran_date, print_content, tran_amt,
                            inout_type, flag, su_amt, cltcd
                        ) VALUES (?,?,?,?,?,?,?,?,0,0,?)
                        """)) {
                        ps.setString(1, custcd);
                        ps.setString(2, msSpjangcd);
                        ps.setString(3, bnkcode);
                        ps.setString(4, item.getMemberNo());
                        ps.setString(5, tranDate + item.getMemberNo());
                        ps.setString(6, tranDate);
                        ps.setString(7, item.getMemberName());
                        ps.setLong(8,   item.getBillingAmount());  // tran_amt = 청구금액
                        ps.setLong(9,   item.getSuAmt());          // su_amt = fee_request + fee_success
                        ps.setString(10, item.getCltcd());
                        ps.execute();
                    }
                    log.info("[CmsErpSync] INSERT 완료 memberNo={}", item.getMemberNo());
                }
                conn.commit();
                log.info("[CmsErpSync] MSSQL 커밋 완료 spjangcd={} {}건", spjangcd, items.size());
            } catch (Exception e) {
                conn.rollback();
                log.error("[CmsErpSync] MSSQL 롤백 spjangcd={}: {}", spjangcd, e.getMessage(), e);
            }
        } catch (Exception e) {
            log.error("[CmsErpSync] MSSQL 연결 실패 spjangcd={}: {}", spjangcd, e.getMessage(), e);
        }
    }

    private String str(Object v) { return v != null ? v.toString() : ""; }
}