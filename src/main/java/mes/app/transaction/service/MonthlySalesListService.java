package mes.app.transaction.service;

import lombok.extern.slf4j.Slf4j;
import mes.app.aspect.DecryptField;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class MonthlySalesListService {

  @Autowired
  SqlRunner sqlRunner;

  public List<Map<String, Object>> getSalesList(String cboYear, Integer cboCompany, String spjangcd) {
    MapSqlParameterSource paramMap = new MapSqlParameterSource();
    paramMap.addValue("cboYear", cboYear);
    paramMap.addValue("cboCompany", cboCompany);
    paramMap.addValue("spjangcd", spjangcd);

    String data_year = cboYear;
    paramMap.addValue("date_form", data_year + "0101");
    paramMap.addValue("date_to", data_year + "1231");

    StringBuilder sql = new StringBuilder();

    // CTE: parsed_sales
    sql.append("""
        WITH parsed_sales AS (
            SELECT
                ts.*,
                TO_CHAR(TO_DATE(ts.misdate, 'YYYYMMDD'), 'MM') AS sales_month
            FROM tb_salesment ts
            WHERE ts.misdate BETWEEN :date_form AND :date_to 
            and ts.spjangcd = :spjangcd
        """);
// and ts.spjangcd = :spjangcd
    // 회사 필터 조건을 CTE 내부에 삽입
    if (cboCompany != null) {
      sql.append(" AND ts.cltcd = :cboCompany");
    }

    sql.append(")\n");

    // SELECT 본문
    sql.append("""
        SELECT
            ps.cltcd,
            c."Name" AS comp_name,
            MAX(sc."Value") AS misgubun,
            ps.iverpernm,
            ps.iverdeptnm
        """);

    // 월별 합계 컬럼 추가 (mon_1 ~ mon_12)
    for (int i = 1; i <= 12; i++) {
      String month = String.format("%02d", i);
      sql.append(",\n  SUM(CASE WHEN sales_month = '").append(month)
          .append("' THEN COALESCE(ps.totalamt, 0) ELSE 0 END) AS mon_")
          .append(i);
    }

    // 총합계 컬럼
    sql.append(",\n  SUM(COALESCE(ps.totalamt, 0)) AS total_sum\n");

    // FROM, JOIN, GROUP BY, ORDER BY 절
    sql.append("""
        FROM parsed_sales ps
        LEFT JOIN company c ON c.id = ps.cltcd
        LEFT JOIN sys_code sc ON sc."Code" = ps.misgubun
        GROUP BY c."Name", ps.iverpernm, ps.iverdeptnm, ps.cltcd
        ORDER BY c."Name", ps.iverpernm, ps.iverdeptnm
        """);

    // 로그 출력
//    log.info("월별 매출현황 (salesment 기준) SQL: {}", sql);
//    log.info("SQL Parameters: {}", paramMap.getValues());

    // 실행 및 반환
    List<Map<String, Object>> items = this.sqlRunner.getRows(sql.toString(), paramMap);
    return items;
  }

  // 입금
  public List<Map<String, Object>> getMonthDepositList(String cboYear, Integer cboCompany, String spjangcd) {
    MapSqlParameterSource paramMap = new MapSqlParameterSource();
    paramMap.addValue("cboYear", cboYear);
    paramMap.addValue("cboCompany", cboCompany);
    paramMap.addValue("spjangcd", spjangcd);

    String data_year = cboYear;
    paramMap.addValue("date_form", data_year + "0101");
    paramMap.addValue("date_to", data_year + "1231");

    StringBuilder sql = new StringBuilder();

    // CTE: parsed_deposit
    sql.append("""
        WITH parsed_deposit AS (
            SELECT
                tb.*,
                TO_CHAR(TO_DATE(tb.trdate, 'YYYYMMDD'), 'MM') AS deposit_month
            FROM tb_banktransit tb
            WHERE tb.ioflag = '0'
              AND tb.trdate BETWEEN :date_form AND :date_to
              AND tb.spjangcd =:spjangcd
        """);

    // 회사 필터 조건을 CTE 내부에 삽입
    if (cboCompany != null) {
      sql.append(" AND tb.cltcd = :cboCompany");
    }

    sql.append(")\n");

    // SELECT 본문 시작
    sql.append("""
        SELECT
            pd.cltcd,
            c."Name" AS comp_name
        """);

    // 월별 합계 컬럼 (mon_1 ~ mon_12)
    for (int i = 1; i <= 12; i++) {
      String month = String.format("%02d", i);
      sql.append(",\n  SUM(CASE WHEN deposit_month = '").append(month)
          .append("' THEN COALESCE(pd.accin, 0) ELSE 0 END) AS mon_").append(i);
    }

    // 총합 컬럼
    sql.append(",\n  SUM(COALESCE(pd.accin, 0)) AS total_sum\n");

    // FROM, JOIN, GROUP BY, ORDER BY
    sql.append("""
        FROM parsed_deposit pd
        LEFT JOIN company c ON c.id = pd.cltcd
        GROUP BY c."Name", pd.cltcd
        ORDER BY c."Name"
        """);

//    log.info("월별 입금현황 SQL: {}", sql);
//    log.info("SQL Parameters: {}", paramMap.getValues());

    List<Map<String, Object>> items = this.sqlRunner.getRows(sql.toString(), paramMap);
    return items;
  }

  //미수금
  public List<Map<String, Object>> getMonthReceivableList(String cboYear, Integer cboCompany, String spjangcd) {
    MapSqlParameterSource paramMap = new MapSqlParameterSource();
    paramMap.addValue("cboYear", cboYear);
    paramMap.addValue("cboCompany", cboCompany);
    paramMap.addValue("spjangcd", spjangcd);

    String baseYm = cboYear + "01";
    String yearStart = cboYear + "0101";
    String yearEnd = cboYear + "1231";

    paramMap.addValue("baseYm", baseYm);
    paramMap.addValue("yearStart", yearStart);
    paramMap.addValue("date_form", yearStart);
    paramMap.addValue("date_to", yearEnd);
    paramMap.addValue("year", cboYear);
    paramMap.addValue("prevYear", String.valueOf(Integer.parseInt(cboYear) - 1));

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
        lasttbl AS (
            SELECT y.cltcd, y.yearamt, y.yyyymm, y.spjangcd
            FROM tb_yearamt y
            JOIN lastym m ON y.cltcd = m.cltcd AND y.yyyymm = m.yyyymm
            WHERE y.ioflag = '0'
              AND y.spjangcd = :spjangcd
        ),
        saletbl AS (
            SELECT s.cltcd, SUM(s.totalamt) AS totsale, s.spjangcd
            FROM tb_salesment s
            WHERE s.misdate BETWEEN :date_form AND :date_to
              AND s.spjangcd = :spjangcd
            GROUP BY s.cltcd, s.spjangcd
        ),
        incomtbl AS (
            SELECT s.cltcd, SUM(s.accin) AS totaccout, s.spjangcd
            FROM tb_banktransit s
            WHERE s.trdate BETWEEN :date_form AND :date_to
              AND s.spjangcd = :spjangcd
              AND s.ioflag = '0'
            GROUP BY s.cltcd, s.spjangcd
        ),
        union_data_raw AS (
            SELECT
                c.id,
                c."Name" AS comp_name,
                TO_DATE(:prevYear || '1231', 'YYYYMMDD') AS date,
                '전잔액' AS summary,
                COALESCE(h.yearamt, 0) AS amount,
                NULL::numeric AS accin,
                NULL::numeric AS totalamt,
                0 AS remaksseq
            FROM company c
            LEFT JOIN lasttbl h ON c.id = h.cltcd AND c.spjangcd = h.spjangcd
            LEFT JOIN saletbl p ON c.id = p.cltcd AND c.spjangcd = p.spjangcd
            LEFT JOIN incomtbl q ON c.id = q.cltcd AND c.spjangcd = q.spjangcd
            WHERE c.spjangcd = :spjangcd
            UNION ALL
            SELECT
                s.cltcd,
                c."Name" AS comp_name,
                TO_DATE(s.misdate, 'YYYYMMDD'),
                '매출',
                NULL::numeric AS amount,
                NULL::numeric AS accin,
                s.totalamt AS totalamt,
                1 AS remaksseq
            FROM tb_salesment s
            JOIN company c ON c.id = s.cltcd AND c.spjangcd = s.spjangcd
            WHERE s.misdate BETWEEN :date_form AND :date_to
              AND s.spjangcd = :spjangcd
            UNION ALL
            SELECT
                b.cltcd,
                c."Name" AS comp_name,
                TO_DATE(b.trdate, 'YYYYMMDD'),
                '입금액',
                NULL::numeric AS amount,
                b.accin,
                NULL::numeric AS totalamt,
                2 AS remaksseq
            FROM tb_banktransit b
            JOIN company c ON c.id = b.cltcd 
            WHERE TO_DATE(b.trdate, 'YYYYMMDD') BETWEEN TO_DATE(:date_form, 'YYYYMMDD') AND TO_DATE(:date_to, 'YYYYMMDD')
              AND b.spjangcd = :spjangcd
              AND b.ioflag = '0'
        ),
        union_data AS (
            SELECT * FROM union_data_raw
            WHERE 1 = 1
    """);

    if (cboCompany != null) {
      sql.append(" AND id = :cboCompany\n");
    }

    sql.append("""
            ),
       running_balance AS (
             SELECT
               id,
               comp_name,
               amount,
               TO_CHAR(date, 'YYYY-MM') AS yyyymm,
               date,
               summary,
               SUM(
                 COALESCE(amount, 0) + COALESCE(totalamt, 0) - COALESCE(accin, 0)
               ) OVER (
                 PARTITION BY id
                 ORDER BY date,
                   CASE summary
                     WHEN '전잔액' THEN 0
                     WHEN '입금액' THEN 1
                     WHEN '입금' THEN 1
                     WHEN '매출' THEN 2
                     ELSE 3
                   END
                 ROWS UNBOUNDED PRECEDING
               ) AS balance
             FROM union_data
           )
               SELECT
                   id AS cltid,
                   comp_name,
                   amount
    """);

    for (int i = 0; i <= 12; i++) {
      String ym = (i == 0)
          ? ":prevYear || '-12'"
          : String.format(":year || '-%02d'", i);
      sql.append(",\n  MAX(CASE WHEN yyyymm = ").append(ym).append(" THEN balance ELSE 0 END) AS mon_").append(i);
    }

    sql.append("""
        FROM running_balance
        GROUP BY id, comp_name, amount
        HAVING SUM(balance) <> 0
        ORDER BY comp_name;
    """);
//    log.info("월별 미수금 집계 SQL: {}", sql);
//    log.info("SQL Parameters: {}", paramMap.getValues());

    return this.sqlRunner.getRows(sql.toString(), paramMap);
  }

  //매출 상세 내역
  public List<Map<String, Object>> getSalesDetail(String cboYear, Integer cltcd, String spjangcd) {

    MapSqlParameterSource paramMap = new MapSqlParameterSource();
    paramMap.addValue("cboYear", cboYear);
    paramMap.addValue("cltcd", cltcd);
    paramMap.addValue("spjangcd", spjangcd);

    String data_year = cboYear;
    paramMap.addValue("date_form", data_year + "0101");
    paramMap.addValue("date_to", data_year + "1231");

    StringBuilder sql = new StringBuilder();

    sql.append("""
         select 
         TO_CHAR(TO_DATE(s.misdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS misdate,
           s.cltcd,
           sc."Value" AS misgubun,
           d.itemnm,
           d.spec,
           d.qty,
           d. supplycost,
           d. taxtotal,
           d.totalamt
         from tb_salesment s
         LEFT JOIN tb_salesdetail d ON s.misnum = d.misnum
         LEFT JOIN sys_code sc ON sc."Code" = s.misgubun and sc."CodeType" ='sale_type'
         LEFT JOIN company c ON c.id = s.cltcd
         WHERE s.spjangcd = :spjangcd
           AND s.cltcd = :cltcd
           AND s.misdate BETWEEN :date_form AND :date_to
           ORDER BY s.misnum,d.misseq
       """);

    return this.sqlRunner.getRows(sql.toString(), paramMap);
  }

  @DecryptField(columns  = {"accnum"})
  public List<Map<String, Object>> getDepositDetail(String cboYear, Integer cltcd, String spjangcd) {
    MapSqlParameterSource paramMap = new MapSqlParameterSource();
    paramMap.addValue("cboYear", cboYear);
    paramMap.addValue("cltcd", cltcd);
    paramMap.addValue("spjangcd", spjangcd);

    String data_year = cboYear;
    paramMap.addValue("start", data_year + "0101");
    paramMap.addValue("end", data_year + "1231");

    StringBuilder sql = new StringBuilder();

    sql.append("""
        select
           tb.ioid,
           TO_CHAR(TO_DATE(tb.trdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS trdate,
           tb.accin ,
           c."Name" as "CompanyName" ,
           tb.iotype ,
           sc."Value" as deposit_type,
           sc."Code" as deposit_code,
           tb.banknm,
           tb.accnum ,
           tt.tradenm ,
           tb.remark1,
           tb.eumnum,
           CASE
             WHEN LENGTH(TRIM(tb.eumtodt)) = 8 THEN TO_CHAR(TO_DATE(tb.eumtodt, 'YYYYMMDD'), 'YYYY-MM-DD')
             ELSE NULL
           END AS eumtodt,
           tb.memo
           from tb_banktransit tb
           left join company c on c.id = tb.cltcd and tb.spjangcd =  c.spjangcd 
           left join  sys_code sc on sc."Code" = tb.iotype and "CodeType" ='deposit_type'
           left join tb_trade tt on tb.trid = tt.trid and tt.spjangcd = tb.spjangcd
           WHERE tb.ioflag = '0'
           AND TO_DATE(tb.trdate, 'YYYYMMDD') 
          BETWEEN TO_DATE(:start, 'YYYYMMDD') AND TO_DATE(:end, 'YYYYMMDD')
           AND tb.spjangcd =  :spjangcd
          AND tb.cltcd = :cltcd;
       """);

//    log.info("월별 매출현황(입금)__입금 상세내역 SQL: {}", sql);
//    log.info("SQL Parameters: {}", paramMap.getValues());

    return this.sqlRunner.getRows(sql.toString(), paramMap);

  }
}
