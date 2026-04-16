package mes.app.transaction.service;

import lombok.extern.slf4j.Slf4j;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class AccountsReceivableListServie {
  @Autowired
  SqlRunner sqlRunner;

  public List<Map<String, Object>> getTotalList(String start_date, String end_date, Integer company, String spjangcd) {
    MapSqlParameterSource paramMap = new MapSqlParameterSource();

    DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    DateTimeFormatter dbFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");

    LocalDate startDate = LocalDate.parse(start_date, inputFormatter);
    LocalDate endDate = LocalDate.parse(end_date, inputFormatter);

    String formattedStart = startDate.format(dbFormatter);
    String formattedEnd = endDate.format(dbFormatter);

    YearMonth baseYm = YearMonth.from(startDate);
    String baseYmStr = baseYm.format(DateTimeFormatter.ofPattern("yyyyMM"));

    paramMap.addValue("start", formattedStart);
    paramMap.addValue("end", formattedEnd);
    paramMap.addValue("baseYm", baseYmStr);
    paramMap.addValue("spjangcd", spjangcd);

    if (company != null) {
      paramMap.addValue("company", company);
    }

    StringBuilder sql = new StringBuilder();

    sql.append("""
        WITH lastym AS (
            SELECT cltcd, MAX(yyyymm) AS yyyymm
            FROM tb_yearamt
            WHERE yyyymm < :baseYm
              AND ioflag = '0'
              AND spjangcd = :spjangcd
            GROUP BY cltcd
        ),
        last_amt AS (
            SELECT y.cltcd, y.yearamt, y.yyyymm
            FROM tb_yearamt y
            JOIN lastym m ON y.cltcd = m.cltcd AND y.yyyymm = m.yyyymm
            WHERE y.ioflag = '0'
              AND y.spjangcd = :spjangcd
        ),
        post_close_txns AS (
            SELECT
                c.id AS cltcd,
                SUM(COALESCE(s.totalamt, 0)) AS extra_sales,
                SUM(COALESCE(b.accin, 0)) AS extra_accin
            FROM company c
            LEFT JOIN tb_salesment s ON c.id = s.cltcd
                AND s.misdate BETWEEN 
                    TO_CHAR((SELECT TO_DATE(MAX(yyyymm), 'YYYYMM') + interval '1 month' FROM last_amt), 'YYYYMMDD')
                    AND TO_CHAR(TO_DATE(:start, 'YYYYMMDD') - interval '1 day', 'YYYYMMDD')
                AND s.spjangcd = :spjangcd
            LEFT JOIN tb_banktransit b ON c.id = b.cltcd
                AND b.trdate BETWEEN 
                    TO_CHAR((SELECT TO_DATE(MAX(yyyymm), 'YYYYMM') + interval '1 month' FROM last_amt), 'YYYYMMDD')
                    AND TO_CHAR(TO_DATE(:start, 'YYYYMMDD') - interval '1 day', 'YYYYMMDD')
                AND b.ioflag = '0'
                AND b.spjangcd = :spjangcd
            GROUP BY c.id
        ),
        uncalculated_txns AS (
            SELECT
                c.id AS cltcd,
                SUM(COALESCE(s.totalamt, 0)) AS total_sales,
                SUM(COALESCE(b.accin, 0)) AS total_accin
            FROM company c
            LEFT JOIN tb_salesment s ON c.id = s.cltcd
                AND s.misdate < :start
                AND s.spjangcd = :spjangcd
            LEFT JOIN tb_banktransit b ON c.id = b.cltcd
                AND b.trdate < :start
                AND b.ioflag = '0'
                AND b.spjangcd = :spjangcd
            WHERE NOT EXISTS (
                SELECT 1 FROM last_amt y WHERE y.cltcd = c.id
            )
            GROUP BY c.id
        ),
        final_prev_amt AS (
            SELECT
                y.cltcd,
                y.yearamt + COALESCE(p.extra_sales, 0) - COALESCE(p.extra_accin, 0) AS prev_amt
            FROM last_amt y
            LEFT JOIN post_close_txns p ON y.cltcd = p.cltcd
            UNION
            SELECT
                u.cltcd,
                COALESCE(u.total_sales, 0) - COALESCE(u.total_accin, 0)
            FROM uncalculated_txns u
        ),
        sales_amt AS (
            SELECT cltcd, SUM(totalamt) AS sales
            FROM tb_salesment
            WHERE misdate BETWEEN :start AND :end
              AND spjangcd = :spjangcd
            GROUP BY cltcd
        ),
        accin_amt AS (
            SELECT cltcd, SUM(accin) AS accin
            FROM tb_banktransit
            WHERE trdate BETWEEN :start AND :end
              AND ioflag = '0'
              AND spjangcd = :spjangcd
            GROUP BY cltcd
        )
        SELECT
            m.id AS cltcd,
            m."Name" AS clt_name,
            COALESCE(f.prev_amt, 0) AS receivables,
            COALESCE(s.sales, 0) AS sales,
            COALESCE(b.accin, 0) AS "AmountDeposited",
            COALESCE(f.prev_amt, 0) + COALESCE(s.sales, 0) - COALESCE(b.accin, 0) AS balance
        FROM company m
        LEFT JOIN final_prev_amt f ON m.id = f.cltcd
        LEFT JOIN sales_amt s ON m.id = s.cltcd
        LEFT JOIN accin_amt b ON m.id = b.cltcd
        WHERE COALESCE(f.prev_amt, 0) + COALESCE(s.sales, 0) - COALESCE(b.accin, 0) <> 0
    """);

    if (company != null) {
      sql.append(" AND m.id = :company");
    }

    sql.append(" ORDER BY m.\"Name\"");

//    log.info("미수금 집계 read SQL: {}", sql);
//    log.info("SQL Parameters: {}", paramMap.getValues());

    return this.sqlRunner.getRows(sql.toString(), paramMap);
  }

  //미수금 현황 상세
  public List<Map<String, Object>> getDetailList(String start_date, String end_date, String company, String spjangcd) {
    MapSqlParameterSource paramMap = new MapSqlParameterSource();
    DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    DateTimeFormatter dbFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");

    LocalDate startDate = LocalDate.parse(start_date, inputFormatter);
    LocalDate endDate = LocalDate.parse(end_date, inputFormatter);

    String formattedStart = startDate.format(dbFormatter);
    String formattedEnd = endDate.format(dbFormatter);

    YearMonth baseYm = YearMonth.from(startDate);
    String baseYmStr = baseYm.format(DateTimeFormatter.ofPattern("yyyyMM"));
    String baseDate = baseYm.atDay(1).format(dbFormatter);

    paramMap.addValue("baseYm", baseYmStr);
    paramMap.addValue("baseDate", baseDate);
    paramMap.addValue("start", formattedStart);
    paramMap.addValue("end", formattedEnd);
    paramMap.addValue("company", Integer.valueOf(company));
    paramMap.addValue("spjangcd", spjangcd);

    String sql = """
        WITH lastym AS (
            SELECT cltcd, MAX(yyyymm) AS yyyymm
            FROM tb_yearamt
            WHERE yyyymm < :baseYm
              AND ioflag = '0'
              AND spjangcd = :spjangcd
              AND cltcd = :company
            GROUP BY cltcd
        ),
        last_amt AS (
            SELECT y.cltcd, y.yearamt, y.yyyymm
            FROM tb_yearamt y
            JOIN lastym m ON y.cltcd = m.cltcd AND y.yyyymm = m.yyyymm
            WHERE y.ioflag = '0'
              AND y.spjangcd = :spjangcd
        ),
        post_close_txns AS (
            SELECT
                s.cltcd,
                SUM(COALESCE(s.totalamt, 0)) AS extra_sales,
                SUM(COALESCE(b.accin, 0)) AS extra_accin
            FROM tb_salesment s
            LEFT JOIN tb_banktransit b ON s.cltcd = b.cltcd
                AND b.trdate BETWEEN 
                    TO_CHAR((SELECT TO_DATE(MAX(yyyymm), 'YYYYMM') + interval '1 month' FROM last_amt), 'YYYYMMDD')
                    AND TO_CHAR(TO_DATE(:start, 'YYYYMMDD') - interval '1 day', 'YYYYMMDD')
                AND b.ioflag = '0'
                AND b.spjangcd = :spjangcd
            WHERE s.misdate BETWEEN 
                    TO_CHAR((SELECT TO_DATE(MAX(yyyymm), 'YYYYMM') + interval '1 month' FROM last_amt), 'YYYYMMDD')
                    AND TO_CHAR(TO_DATE(:start, 'YYYYMMDD') - interval '1 day', 'YYYYMMDD')
              AND s.spjangcd = :spjangcd
              AND s.cltcd = :company
            GROUP BY s.cltcd
        ),
        uncalculated_txns AS (
            SELECT
                s.cltcd,
                SUM(COALESCE(s.totalamt, 0)) AS total_sales,
                SUM(COALESCE(b.accin, 0)) AS total_accin
            FROM tb_salesment s
            LEFT JOIN tb_banktransit b ON s.cltcd = b.cltcd
                AND b.trdate < :start
                AND b.ioflag = '0'
                AND b.spjangcd = :spjangcd
            WHERE s.misdate < :start
              AND s.spjangcd = :spjangcd
              AND s.cltcd = :company
              AND NOT EXISTS (
                  SELECT 1 FROM last_amt y WHERE y.cltcd = s.cltcd
              )
            GROUP BY s.cltcd
        ),
        final_prev_amt AS (
            SELECT
                y.cltcd,
                y.yearamt + COALESCE(p.extra_sales, 0) - COALESCE(p.extra_accin, 0) AS prev_amt
            FROM last_amt y
            LEFT JOIN post_close_txns p ON y.cltcd = p.cltcd
            UNION
            SELECT
                u.cltcd,
                COALESCE(u.total_sales, 0) - COALESCE(u.total_accin, 0)
            FROM uncalculated_txns u
        ),
        union_data_raw AS (
            -- 전잔액
            SELECT
                c.id AS cltcd,
                c."Name" AS comp_name,
                TO_DATE(:baseDate, 'YYYYMMDD') AS date,
                '전잔액' AS summary,
                COALESCE(f.prev_amt, 0) AS amount,
                NULL::text AS itemnm,
                NULL::text AS misgubun,
                NULL::text AS iotype,
                NULL::text AS banknm,
                NULL::text AS accnum,
                NULL::text AS eumnum,
                NULL::text AS eumtodt,
                NULL::text AS tradenm,
                NULL::numeric AS accin,
                NULL::numeric AS totalamt,
                NULL::text AS memo,
                NULL::text AS remark1,
                0 AS remaksseq
            FROM company c
            LEFT JOIN final_prev_amt f ON c.id = f.cltcd
            WHERE c.id = :company AND c.spjangcd = :spjangcd
            UNION ALL
            -- 매출
            SELECT
                s.cltcd,
                c."Name" AS comp_name,
                TO_DATE(s.misdate, 'YYYYMMDD') AS date,
                '매출' AS summary,
                NULL::numeric AS amount,
                CONCAT(
                    MAX(CASE WHEN d.misseq::int = 1 THEN d.itemnm END),
                    CASE WHEN COUNT(DISTINCT d.itemnm) > 1 THEN ' 외 ' || (COUNT(DISTINCT d.itemnm) - 1) || '건' ELSE '' END
                ) AS itemnm,
                sc."Value" AS misgubun,
                NULL::text AS iotype,
                NULL::text AS banknm,
                NULL::text AS accnum,
                NULL::text AS eumnum,
                NULL::text AS eumtodt,
                NULL::text AS tradenm,
                NULL::numeric AS accin,
                s.totalamt,
                NULL::text AS memo,
                s.remark1,
                2 AS remaksseq
            FROM tb_salesment s
            LEFT JOIN tb_salesdetail d ON s.misdate = d.misdate AND s.misnum = d.misnum AND s.spjangcd = d.spjangcd
            LEFT JOIN sys_code sc ON sc."Code" = s.misgubun
            JOIN company c ON c.id = s.cltcd 
            WHERE s.misdate BETWEEN :start AND :end
              AND s.cltcd = :company
              AND s.spjangcd = :spjangcd
            GROUP BY s.cltcd, c."Name", s.misdate, s.misnum, s.totalamt, s.misgubun, sc."Value", s.remark1
            UNION ALL
            -- 입금
            SELECT
                b.cltcd,
                c."Name" AS comp_name,
                TO_DATE(b.trdate, 'YYYYMMDD') AS date,
                '입금' AS summary,
                NULL::numeric AS amount,
                NULL::text AS itemnm,
                NULL::text AS misgubun,
                sc."Value" AS iotype,
                b.banknm,
                b.accnum,
                b.eumnum,
                TO_CHAR(TO_DATE(NULLIF(b.eumtodt, ''), 'YYYYMMDD'), 'YYYY-MM-DD') AS eumtodt,
                tt.tradenm,
                b.accin,
                NULL::numeric AS totalamt,
                b.memo,
                b.remark1,
                1 AS remaksseq
            FROM tb_banktransit b
            JOIN company c ON c.id = b.cltcd 
            LEFT JOIN sys_code sc ON sc."Code" = b.iotype and sc."CodeType" = 'deposit_type'
            LEFT JOIN tb_trade tt ON tt.trid = b.trid AND tt.spjangcd = b.spjangcd
            WHERE TO_DATE(b.trdate, 'YYYYMMDD') BETWEEN TO_DATE(:start, 'YYYYMMDD') AND TO_DATE(:end, 'YYYYMMDD') 
              AND b.cltcd = :company
              AND b.spjangcd = :spjangcd
              AND b.ioflag = '0'
        )
        SELECT
            x.cltcd,
            x.comp_name,
            x.date,
            x.summary,
            x.amount,
            COALESCE(x.amount, x.totalamt, x.accin) AS total_amount,
            SUM(
              COALESCE(x.amount, 0) + COALESCE(x.totalamt, 0) - COALESCE(x.accin, 0)
            ) OVER (
              PARTITION BY x.cltcd
              ORDER BY x.date, x.remaksseq, x.itemnm
              ROWS UNBOUNDED PRECEDING
            ) AS balance,
            x.accin,
            x.totalamt,
            x.itemnm,
            x.misgubun,
            x.iotype,
            x.banknm,
            x.accnum,
            x.eumnum,
            x.eumtodt,
            x.tradenm,
            x.memo,
            x.remark1
        FROM union_data_raw x
        ORDER BY x.cltcd, x.date, x.remaksseq
        """;

//    log.info("미수금 상세 read SQL: {}", sql);
//    log.info("SQL Parameters: {}", paramMap.getValues());

    return this.sqlRunner.getRows(sql, paramMap);
  }

}
