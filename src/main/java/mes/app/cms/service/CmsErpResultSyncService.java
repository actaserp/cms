package mes.app.cms.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mes.domain.services.SqlRunner;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class CmsErpResultSyncService {

    private final SqlRunner sqlRunner;

    public void syncResult(String spjangcd, String targetDate,
                           String memberNo, String memberName, String bankAccount,
                           int lineSeq, boolean success, String disableCd, long suAmt) {
        // ERP 접속정보 조회
        Map<String, Object> erp = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT host, port, db_name, username, password, ms_spjangcd FROM tb_xa012_erp WHERE spjangcd = :spjangcd",
                new MapSqlParameterSource("spjangcd", spjangcd));
        if (erp == null) return; // ERP 미연동이면 스킵

        String msSpjangcd = str(erp.get("ms_spjangcd"));
        String reqDate    = targetDate.substring(2); // YYYYMMDD → YYMMDD
        String ipDate     = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String endflag    = success ? "Y" : "N";

        String url = String.format("jdbc:sqlserver://%s:%s;databaseName=%s;encrypt=false",
                str(erp.get("host")), str(erp.get("port")), str(erp.get("db_name")));

        try { Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver"); }
        catch (ClassNotFoundException e) { throw new IllegalStateException("MSSQL 드라이버 없음"); }

        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(
                url, str(erp.get("username")), str(erp.get("password")))) {

            // custcd 조회
            Map<String, Object> erpInfo = sqlRunner.getRow(/* skip_tenant_check */
                    "SELECT custcd FROM tb_xa012_erp WHERE spjangcd = :spjangcd",
                    new MapSqlParameterSource("spjangcd", spjangcd));
            String custcd = str(erpInfo != null ? erpInfo.get("custcd") : "");

// cmar 조회 (TB_XENV)
            long cmar = 0;
            try (java.sql.PreparedStatement ps = conn.prepareStatement(
                    "SELECT cmar FROM TB_XENV WHERE spjangcd = ? AND custcd = ?")) {
                ps.setString(1, msSpjangcd);
                ps.setString(2, custcd);
                try (java.sql.ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) cmar = rs.getLong("cmar");
                }
            }

            if (success) {
                String bnkcode = reqDate.substring(0, 2) + reqDate.substring(2, 6)
                        + String.format("%04d", lineSeq); // YYMMDD → YY+MMDD+순번4자리
                String fintech = targetDate + String.format("%04d", lineSeq); // YYYYMMDD+순번4자리

                try (java.sql.PreparedStatement ps = conn.prepareStatement("""
            INSERT INTO TB_BANK_CMSSAVE (
                custcd, spjangcd, bnkcode, cmsnum, fintech_use_num,
                tran_date, inout_type, print_content,
                tran_amt, su_amt, flag, ipdate,
                misdate, misnum, cltcd, cmsaccnum
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """)) {
                    ps.setString(1,  custcd);
                    ps.setString(2,  msSpjangcd);
                    ps.setString(3,  bnkcode);
                    ps.setString(4,  bankAccount);
                    ps.setString(5,  fintech);
                    ps.setString(6,  "20" + reqDate);
                    ps.setString(7,  "0");
                    ps.setString(8,  memberNo); // member_name이 필요하면 파라미터 추가
                    ps.setLong(9,    suAmt > 0 ? suAmt : cmar); // suAmt 우선, 없으면 cmar
                    ps.setLong(10,   cmar);
                    ps.setString(11, "1");
                    ps.setString(12, ipDate);
                    ps.setString(13, targetDate);
                    ps.setString(14, String.format("%04d", lineSeq));
                    ps.setString(15, memberNo);
                    ps.setString(16, bankAccount);
                    ps.execute();
                }
            } else {
                // 실패는 TB_BANK_CMSSAVE INSERT 안 함 — 필요하면 별도 처리
                log.info("[CmsErpSync] 출금실패 INSERT 스킵 memberNo={} disableCd={}", memberNo, disableCd);
            }
        } catch (Exception e) {
            log.error("[CmsErpSync] 실패 spjangcd={} memberNo={}: {}", spjangcd, memberNo, e.getMessage());
        }
    }

    private String str(Object v) { return v != null ? v.toString() : ""; }
}