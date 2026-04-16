package mes.app.definition.service;

import lombok.extern.slf4j.Slf4j;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class YearamtService {

    @Autowired
    SqlRunner sqlRunner;

    @Autowired
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    public List<Map<String, Object>> getYearamtList(
        String year, String ioflag, String cltid, String name, String endyn, String spjangcd) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource()
            .addValue("year", year)
            .addValue("ioflag", ioflag)          // "0" or "1" (문자열로 사용)
            .addValue("searchid", cltid)
            .addValue("name", name)
            .addValue("endyn", endyn)            // "Y" / "N" (N이면 미마감 + NULL)
            .addValue("spjangcd", spjangcd);

        int targetYear = Integer.parseInt(year) - 1;
        String yyyymm = targetYear + "12";       // 전년도 12월
        dicParam.addValue("yyyymm", yyyymm);

        String sql;
        if ("0".equals(ioflag)) {
            // 매출
            sql = """
                    SELECT
                      c.id,
                      c."Name" AS company_name,
                      COALESCE(
                        COALESCE(y.yearamt, 0) + COALESCE(s.totalamt_sum, 0) - COALESCE(b.accin_sum, 0),
                        0
                      ) AS balance,
                      COALESCE(y.ioflag, :ioflag) AS ioflag,
                      :year || '12' AS yyyymm,
                      COALESCE(m.endyn, 'N') AS endyn
                    FROM company c
                    /* 전년도 12월 확정(개시잔액) */
                    LEFT JOIN (
                      SELECT cltcd, yearamt, ioflag
                      FROM tb_yearamt
                      WHERE yyyymm = :yyyymm
                        AND spjangcd = :spjangcd
                        AND ioflag   = :ioflag
                    ) y ON y.cltcd = c.id
                    /* 올해 마감 여부 */
                    LEFT JOIN (
                      SELECT cltcd, MAX(endyn) AS endyn
                      FROM tb_yearamt
                      WHERE yyyymm   = :year || '12'
                        AND ioflag   = :ioflag
                        AND spjangcd = :spjangcd
                      GROUP BY cltcd
                    ) m ON m.cltcd = c.id
                    /* 올해 매출 합계 */
                    LEFT JOIN (
                      SELECT cltcd, SUM(totalamt) AS totalamt_sum
                      FROM tb_salesment
                      WHERE misdate BETWEEN '20000101' AND :year || '1231'
                        AND spjangcd = :spjangcd
                      GROUP BY cltcd
                    ) s ON s.cltcd = c.id
                    /* 올해 입금 합계(매출쪽) */
                    LEFT JOIN (
                      SELECT cltcd, SUM(accin) AS accin_sum
                      FROM tb_banktransit
                      WHERE trdate BETWEEN '20000101' AND :year || '1231'
                        AND spjangcd = :spjangcd
                        AND ioflag = '0'
                      GROUP BY cltcd
                    ) b ON b.cltcd = c.id
                    WHERE c.relyn = '0'
                      AND c.id::text LIKE concat('%', :searchid, '%')
                      AND c."Name"  LIKE concat('%', :name, '%')
                      AND c.spjangcd = :spjangcd
                      AND COALESCE(m.endyn, 'N') = :endyn
                    ORDER BY c."Name"
                """;
        } else {
            // 매입
            sql = """
                    WITH client AS (
                         SELECT id, '0' AS cltflag, "Name" AS cltname
                         FROM company WHERE spjangcd = :spjangcd
                         UNION ALL
                         SELECT id, '1' AS cltflag, "Name" AS cltname
                         FROM person WHERE spjangcd = :spjangcd
                         UNION ALL
                         SELECT bankid AS id, '2' AS cltflag, banknm AS cltname
                         FROM tb_xbank WHERE spjangcd = :spjangcd
                         UNION ALL
                         SELECT id, '3' AS cltflag, cardnm AS cltname
                         FROM tb_iz010 WHERE spjangcd = :spjangcd
                     ),
                     yearamt AS (
                         SELECT cltcd, yearamt, ioflag
                         FROM tb_yearamt
                         WHERE yyyymm = :yyyymm
                           AND spjangcd = :spjangcd
                     ),
                     end_flag AS (
                         SELECT cltcd, MAX(endyn) AS endyn
                         FROM tb_yearamt
                         WHERE yyyymm   = :year || '12'
                           AND ioflag   = :ioflag
                           AND spjangcd = :spjangcd
                         GROUP BY cltcd
                     ),
                     invo_sum AS (
                         SELECT cltcd, SUM(totalamt) AS totalamt_sum
                         FROM tb_invoicement
                         WHERE misdate BETWEEN '20000101' AND :year || '1231'
                           AND spjangcd = :spjangcd
                         GROUP BY cltcd
                     ),
                     bank_sum AS (
                         SELECT cltcd, SUM(accout) AS accout_sum
                         FROM tb_banktransit
                         WHERE trdate BETWEEN '20000101' AND :year || '1231'
                           AND spjangcd = :spjangcd
                           AND ioflag = '1'
                         GROUP BY cltcd
                     )
                     SELECT
                         c.id,
                         c.cltflag,
                         CASE c.cltflag
                               WHEN '0' THEN '업체'
                               WHEN '1' THEN '직원정보'
                               WHEN '2' THEN '은행계좌'
                               WHEN '3' THEN '카드사'
                         END AS cltflagnm,
                         c.cltname AS company_name,
                         COALESCE(
                             COALESCE(y.yearamt, 0) + COALESCE(s.totalamt_sum, 0) - COALESCE(b.accout_sum, 0),
                             0
                         ) AS balance,
                         COALESCE(y.ioflag, :ioflag) AS ioflag,
                         :year || '12' AS yyyymm,
                         COALESCE(m.endyn, 'N') AS endyn
                     FROM client c
                     LEFT JOIN yearamt y  ON y.cltcd = c.id
                     LEFT JOIN end_flag m ON m.cltcd = c.id
                     LEFT JOIN invo_sum s ON s.cltcd = c.id
                     LEFT JOIN bank_sum b ON b.cltcd = c.id
                     WHERE c.id::text LIKE concat('%', :searchid, '%')
                       AND c.cltname LIKE concat('%', :name, '%')
                       AND COALESCE(m.endyn, 'N') = :endyn
                     ORDER BY c.cltflag,  c.cltname
                """;
        }

//        log.info("매입매출 년마감 SQL: {}", sql);
//        log.info("SQL Parameters: {}", dicParam.getValues());

        return sqlRunner.getRows(sql, dicParam);
    }

}
