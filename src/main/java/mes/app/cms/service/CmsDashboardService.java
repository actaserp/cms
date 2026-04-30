package mes.app.cms.service;

import mes.app.common.TenantContext;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class CmsDashboardService {

    @Autowired
    SqlRunner sqlRunner;

    public Map<String, Object> getSummary(String ym) {
        String spjangcd = TenantContext.get();
        var param = new MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);
        param.addValue("ymStart", ym + "01");
        param.addValue("ymEnd",   ym + "31");

        String sql = """
        SELECT COUNT(*)                                                          AS total_count,
               COALESCE(SUM(billing_amount), 0)                                 AS total_amount,
               COUNT(*) FILTER (WHERE status IN ('FAIL', 'ERROR'))               AS fail_count,
               ROUND(
                   COUNT(*) FILTER (WHERE status = 'SUCCESS') * 100.0
                   / NULLIF(COUNT(*) FILTER (WHERE status IN ('SUCCESS', 'FAIL')), 0),
               1)                                                                AS success_rate
        FROM cms_billing
        WHERE spjangcd   = :spjangcd
          AND deduct_date BETWEEN :ymStart AND :ymEnd
        """;
        return sqlRunner.getRow(sql, param);
    }

    public List<Map<String, Object>> getCalendarData(String ym) {
        String spjangcd = TenantContext.get();
        var param = new MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);
        param.addValue("ymStart", ym + "01");
        param.addValue("ymEnd",   ym + "31");

        String sql = """
        SELECT deduct_date,
               COUNT(*)                                              AS total_count,
               COALESCE(SUM(billing_amount), 0)                     AS total_amount,
               COUNT(*) FILTER (WHERE status IN ('FAIL', 'ERROR'))   AS fail_count,
               COUNT(*) FILTER (WHERE status = 'PENDING')            AS pending_count
        FROM cms_billing
        WHERE spjangcd   = :spjangcd
          AND deduct_date BETWEEN :ymStart AND :ymEnd
        GROUP BY deduct_date
        ORDER BY deduct_date
        """;
        return sqlRunner.getRows(sql, param);
    }

    public List<Map<String, Object>> getDailyList(String date) {
        String spjangcd = TenantContext.get();
        var param = new MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);
        param.addValue("date", date);

        String sql = """
            SELECT b.billing_seq,
                   b.member_name,
                   bc.bank_name,
                   b.billing_amount,
                   b.status,
                   b.result_date,
                   b.deduct_type
            FROM cms_billing b
            LEFT JOIN cms_bank_code bc ON bc.bank_code = b.bank_code
            WHERE b.spjangcd   = :spjangcd
              AND b.deduct_date = :date
            ORDER BY b.billing_seq
            """;
        return sqlRunner.getRows(sql, param);
    }
}
