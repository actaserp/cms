package mes.app.transaction.service;

import lombok.extern.slf4j.Slf4j;
import mes.app.aspect.DecryptField;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.sql.Types;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class MonthlyPurchaseListService {

  @Autowired
  SqlRunner sqlRunner;

  public List<Map<String, Object>> getMonthDepositList(String cboYear, Integer cboCompany, String spjangcd, String cltflag) {
    MapSqlParameterSource paramMap = new MapSqlParameterSource();
    paramMap.addValue("cboYear", cboYear);
    paramMap.addValue("cboCompany", cboCompany);
    paramMap.addValue("spjangcd", spjangcd);
    paramMap.addValue("cboCltFlag", cltflag);

    String data_year = cboYear;
    paramMap.addValue("date_form", data_year + "0101");
    paramMap.addValue("date_to", data_year + "1231");

    StringBuilder sql = new StringBuilder();

    sql.append("""
    WITH client AS (
        SELECT id, '0' AS cltflag, "Name" AS cltname FROM company WHERE spjangcd = :spjangcd
        UNION ALL
        SELECT id, '1' AS cltflag, "Name" AS cltname FROM person WHERE spjangcd = :spjangcd
        UNION ALL
        SELECT bankid AS id, '2' AS cltflag, banknm AS cltname FROM tb_xbank WHERE spjangcd = :spjangcd
        UNION ALL
        SELECT id, '3' AS cltflag, cardnm AS cltname FROM tb_iz010 WHERE spjangcd = :spjangcd
    ),
    invoicedata AS (
      SELECT 
          i.cltcd,
          i.cltflag,
          i.misgubun,
          s."Value" AS misgubun2,
          i.depart_id,
          d."Name" AS depart_name,
          i.misdate,
          i.misnum,
          c."AccountManager" as account_manager, 
          TO_CHAR(TO_DATE(i.misdate, 'YYYYMMDD'), 'MM') AS sale_month,
          COALESCE(tid.supplycost, 0) AS supplycost,
          COALESCE(tid.taxtotal, 0) AS taxtotal,
          COALESCE(tid.supplycost, 0) + COALESCE(tid.taxtotal, 0) AS totalamt
      FROM tb_invoicement i
      LEFT JOIN tb_invoicedetail tid 
          ON i.misdate = tid.misdate 
         AND i.misnum = tid.misnum 
         AND i.spjangcd = tid.spjangcd
      LEFT JOIN depart d 
          ON d.id = i.depart_id
      LEFT JOIN sys_code s 
          ON s."Code" = i.misgubun AND s."CodeType" = 'purchase_type'
      LEFT JOIN company c on c.id = i.cltcd 
      WHERE i.misdate BETWEEN :date_form AND :date_to
        AND i.spjangcd = :spjangcd
    """);

    if (cboCompany != null && cltflag != null) {
      sql.append(" AND i.cltcd = :cboCompany AND i.cltflag = :cboCltFlag");
    }

    sql.append("""
    )
    SELECT
        i.cltcd,
        i.cltflag,
        c.cltname,
        i.misgubun,
        i.misgubun2 AS misgubun_name,
        i.depart_id,
        i.depart_name,
        i.misdate,
        i.misnum,
        i.account_manager
    """);

    for (int i = 1; i <= 12; i++) {
      String month = String.format("%02d", i);
      sql.append(",\n  SUM(CASE WHEN sale_month = '").append(month)
          .append("' THEN totalamt ELSE 0 END) AS mon_").append(i);
    }

    sql.append(",\n  SUM(totalamt) AS total_sum\n");

    sql.append("""
    FROM invoicedata i
    LEFT JOIN client c ON c.id = i.cltcd AND c.cltflag = i.cltflag
    GROUP BY 
        i.cltcd, i.cltflag, c.cltname,
        i.misgubun, i.misgubun2,
        i.depart_id, i.depart_name,
        i.misdate, i.misnum, i.account_manager
    ORDER BY i.misdate DESC, i.misnum DESC
    """);

//    log.info("월별 매입현황 SQL: {}", sql);
//    log.info("SQL 월별 매입현황 Parameters: {}", paramMap.getValues());

    return this.sqlRunner.getRows(sql.toString(), paramMap);
  }

  public List<Map<String, Object>> getProvisionList(String cboYear, Integer cboCompany, String spjangcd) {
    MapSqlParameterSource paramMap = new MapSqlParameterSource();
    paramMap.addValue("cboYear", cboYear);
    paramMap.addValue("cboCompany", cboCompany);
    paramMap.addValue("spjangcd", spjangcd);

    String data_year = cboYear;
    paramMap.addValue("date_form", data_year + "0101");
    paramMap.addValue("date_to", data_year + "1231");

    StringBuilder sql = new StringBuilder();

    // CTE: client + parsed_deposit
    sql.append("""
        WITH client AS (
            SELECT id, '0' AS cltflag, "Name" AS cltname
            FROM company
            WHERE spjangcd = :spjangcd

            UNION ALL

            SELECT id, '1' AS cltflag, "Name" AS cltname
            FROM person
            WHERE spjangcd = :spjangcd

            UNION ALL

            SELECT bankid AS id, '2' AS cltflag, banknm AS cltname
            FROM tb_xbank
            WHERE spjangcd = :spjangcd

            UNION ALL

            SELECT id, '3' AS cltflag, cardnm AS cltname
            FROM tb_iz010
            WHERE spjangcd = :spjangcd
        ),
        parsed_deposit AS (
            SELECT
                tb.*,
                TO_CHAR(TO_DATE(tb.trdate, 'YYYYMMDD'), 'MM') AS deposit_month
            FROM tb_banktransit tb
            WHERE tb.ioflag = '1'
              AND tb.trdate BETWEEN :date_form AND :date_to
              AND tb.spjangcd = :spjangcd
        """);

    if (cboCompany != null) {
      sql.append(" AND tb.cltcd = :cboCompany");
    }

    sql.append(")\n");

    // SELECT 절
    sql.append("""
        SELECT
            pd.cltcd,
            pd.cltflag,
            c.cltname AS comp_name
        """);

    for (int i = 1; i <= 12; i++) {
      String month = String.format("%02d", i);
      sql.append(",\n  SUM(CASE WHEN deposit_month = '").append(month)
          .append("' THEN COALESCE(pd.accout, 0) ELSE 0 END) AS mon_").append(i);
    }

    sql.append(",\n  SUM(COALESCE(pd.accout, 0)) AS total_sum\n");

    sql.append("""
        FROM parsed_deposit pd
        LEFT JOIN client c ON c.id = pd.cltcd AND c.cltflag = pd.cltflag
        GROUP BY pd.cltcd, pd.cltflag, c.cltname
        ORDER BY c.cltname
        """);

//    log.info("월별 지급현황 SQL: {}", sql);
//    log.info("SQL Parameters: {}", paramMap.getValues());

    return this.sqlRunner.getRows(sql.toString(), paramMap);
  }

  // 미지급
  public List<Map<String, Object>> getMonthPayableList(String cboYear, Integer cboCompany, String spjangcd, String cltflag) {
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

    if (cltflag != null && !cltflag.isBlank()) {
      paramMap.addValue("cltflag", cltflag);
    } else {
      paramMap.addValue("cltflag", null, Types.VARCHAR);
    }

    StringBuilder sql = new StringBuilder();

    sql.append("""
        WITH client AS (
                SELECT id, '0' AS cltflag, "Name" AS cltname FROM company WHERE spjangcd = :spjangcd
                UNION ALL
                SELECT id, '1' AS cltflag, "Name" AS cltname FROM person WHERE spjangcd = :spjangcd
                UNION ALL
                SELECT bankid AS id, '2' AS cltflag, banknm AS cltname FROM tb_xbank WHERE spjangcd = :spjangcd
                UNION ALL
                SELECT id, '3' AS cltflag, cardnm AS cltname FROM tb_iz010 WHERE spjangcd = :spjangcd
            ),
            lastym AS (
                SELECT cltcd, MAX(yyyymm) AS yyyymm
                FROM tb_yearamt
                WHERE yyyymm < :baseYm
                  AND ioflag = '1'
                  AND spjangcd = :spjangcd
                GROUP BY cltcd
            ),
            lasttbl AS (
                SELECT y.cltcd, y.yearamt, y.yyyymm, y.spjangcd
                FROM tb_yearamt y
                JOIN lastym m ON y.cltcd = m.cltcd AND y.yyyymm = m.yyyymm
                WHERE y.ioflag = '1'
                  AND y.spjangcd = :spjangcd
            ),
            purchasetbl AS (
                SELECT s.cltcd, SUM(s.totalamt) AS totpurchase, s.spjangcd
                FROM tb_invoicement s
                WHERE s.misdate BETWEEN :date_form AND :date_to
                  AND s.spjangcd = :spjangcd
                GROUP BY s.cltcd, s.spjangcd
            ),
            paytbl AS (
                SELECT s.cltcd, SUM(s.accout) AS totpayment, s.spjangcd
                FROM tb_banktransit s
                WHERE s.trdate BETWEEN :date_form AND :date_to
                  AND s.spjangcd = :spjangcd
                  AND s.ioflag = '1'
                GROUP BY s.cltcd, s.spjangcd
            ),
            union_data_raw AS (
                SELECT
                    c.id AS id,
                    c.cltflag,
                    c.cltname AS comp_name,
                    TO_DATE(:prevYear || '1231', 'YYYYMMDD') AS date,
                    '전잔액' AS summary,
                    COALESCE(h.yearamt, 0) AS amount,
                    NULL::numeric AS accout,
                    NULL::numeric AS totalamt,
                    0 AS remaksseq
                FROM client c
                LEFT JOIN lasttbl h ON c.id = h.cltcd AND h.spjangcd = :spjangcd
                LEFT JOIN purchasetbl p ON c.id = p.cltcd AND p.spjangcd = :spjangcd
                LEFT JOIN paytbl q ON c.id = q.cltcd AND q.spjangcd = :spjangcd
                WHERE (:cltflag IS NULL OR c.cltflag = :cltflag)
                UNION ALL
                SELECT
                    s.cltcd AS id,
                    c.cltflag,
                    c.cltname AS comp_name,
                    TO_DATE(s.misdate, 'YYYYMMDD'),
                    '매입',
                    NULL::numeric AS amount,
                    NULL::numeric AS accout,
                    s.totalamt AS totalamt,
                    1 AS remaksseq
                FROM tb_invoicement s
                JOIN client c ON c.id = s.cltcd
                WHERE s.misdate BETWEEN :date_form AND :date_to
                  AND s.spjangcd = :spjangcd
                  AND (:cltflag IS NULL OR c.cltflag = :cltflag)
                UNION ALL
                SELECT
                    b.cltcd AS id,
                    c.cltflag,
                    c.cltname AS comp_name,
                    TO_DATE(b.trdate, 'YYYYMMDD'),
                    '지급액',
                    NULL::numeric AS amount,
                    b.accout,
                    NULL::numeric AS totalamt,
                    2 AS remaksseq
                FROM tb_banktransit b
                JOIN client c ON c.id = b.cltcd
                WHERE TO_DATE(b.trdate, 'YYYYMMDD') BETWEEN TO_DATE(:date_form, 'YYYYMMDD') AND TO_DATE(:date_to, 'YYYYMMDD')
                  AND b.spjangcd = :spjangcd
                  AND b.ioflag = '1'
                  AND (:cltflag IS NULL OR c.cltflag = :cltflag)
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
                cltflag,
                comp_name,
                amount,
                TO_CHAR(date, 'YYYY-MM') AS yyyymm,
                date,
                summary,
                SUM(
               COALESCE(amount, 0) + COALESCE(totalamt, 0) - COALESCE(accout, 0)
             ) OVER (
               PARTITION BY id
               ORDER BY date,
                 CASE summary
                   WHEN '전잔액' THEN 0
                   WHEN '지급액' THEN 1
                   WHEN '지급' THEN 1
                   WHEN '매입' THEN 2
                   ELSE 3
                 END
               ROWS UNBOUNDED PRECEDING
             ) AS balance
             FROM union_data
        )
        SELECT
            id AS cltid,
            cltflag,
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
       GROUP BY id, cltflag, comp_name, amount
       HAVING SUM(balance) <> 0
       ORDER BY comp_name
    """);
//    log.info("미지급금 월별 현황 SQL: {}", sql);
//    log.info("SQL Parameters: {}", paramMap.getValues());
    return this.sqlRunner.getRows(sql.toString(), paramMap);
  }

  public List<Map<String, Object>> getPurchaseDetail(String cboYear, Integer cltcd, String spjangcd, Integer depart_id, String cltflag) {
    MapSqlParameterSource paramMap = new MapSqlParameterSource();
    paramMap.addValue("cboYear", cboYear);
    paramMap.addValue("cltcd", cltcd);
    paramMap.addValue("cltflag", cltflag);
    paramMap.addValue("spjangcd", spjangcd);
    paramMap.addValue("depart_id", depart_id);

    String data_year = cboYear;
    paramMap.addValue("date_form", data_year + "0101");
    paramMap.addValue("date_to", data_year + "1231");

    StringBuilder sql = new StringBuilder();

    sql.append("""
       select 
        TO_CHAR(TO_DATE(s.misdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS misdate,
          s.misnum,
          d.misseq ,
          s.cltcd,
          sc."Value" AS misgubun,
          d.itemnm,
          d.spec,
          d.qty,
          d. supplycost,
          d. taxtotal,
          d.totalamt
        from tb_invoicement s
        LEFT JOIN tb_invoicedetail d ON s.misnum = d.misnum
        LEFT JOIN sys_code sc ON sc."Code" = s.misgubun
        LEFT JOIN company c ON c.id = s.cltcd
        WHERE s.spjangcd = :spjangcd
          AND s.cltcd = :cltcd
          and s.depart_id = :depart_id
          and s.cltflag = :cltflag
          AND s.misdate BETWEEN :date_form AND :date_to
          ORDER BY s.misnum ,d.misseq 
       """);

//    log.warn("매입 상세 내역 SQL: {}", sql);
//    log.info("SQL 매입 상세 내역 Parameters: {}", paramMap.getValues());
    return this.sqlRunner.getRows(sql.toString(), paramMap);
  }

  @DecryptField(columns  = {"accnum"})
  public List<Map<String, Object>> getPaymentDetail(String cboYear, Integer cltcd, String spjangcd, String cltflag) {
    MapSqlParameterSource paramMap = new MapSqlParameterSource();
    paramMap.addValue("cboYear", cboYear);
    paramMap.addValue("cltcd", cltcd);
    paramMap.addValue("spjangcd", spjangcd);
    paramMap.addValue("cltflag", cltflag);

    String data_year = cboYear;
    paramMap.addValue("start", data_year + "0101");
    paramMap.addValue("end", data_year + "1231");

    StringBuilder sql = new StringBuilder();

    sql.append("""
        select
           tb.ioid,
           TO_CHAR(TO_DATE(tb.trdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS trdate,
           tb.accout ,
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
           WHERE tb.ioflag = '1'
           AND TO_DATE(tb.trdate, 'YYYYMMDD') 
          BETWEEN TO_DATE(:start, 'YYYYMMDD') AND TO_DATE(:end, 'YYYYMMDD')
           AND tb.spjangcd =  :spjangcd
            and tb.cltflag = :cltflag
          AND tb.cltcd = :cltcd
       """);

//    log.info("월별 매입현황(지급)__지급 상세내역 SQL: {}", sql);
//    log.info("SQL Parameters: {}", paramMap.getValues());

    return this.sqlRunner.getRows(sql.toString(), paramMap);
  }
}

