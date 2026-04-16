package mes.app.transaction.service;

import lombok.extern.slf4j.Slf4j;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class ProjectStatusService {

  @Autowired
  SqlRunner sqlRunner;


  public List<Map<String, Object>> getProjectStatusList(String spjangcd, String txtProjectName, String cboYear) {

    MapSqlParameterSource dicParam = new MapSqlParameterSource();
    dicParam.addValue("spjangcd", spjangcd);

    String sql = """
        WITH
        suju_sum AS (
            SELECT
                project_id,
                spjangcd,
                SUM(COALESCE("TotalAmount", 0)) AS suju_totalamt
            FROM suju
            GROUP BY project_id, spjangcd
        ),
        sales_sum AS (
            SELECT
                projectcode,
                spjangcd,
                SUM(COALESCE(totalamt, 0)) AS sales_totalamt
            FROM tb_salesment
            GROUP BY projectcode, spjangcd
        ),
        invo_sum AS (
            SELECT
                projcd,
                spjangcd,
                SUM(COALESCE(supplycost, 0) + COALESCE(taxtotal, 0)) AS invo_totalamt
            FROM tb_invoicedetail
            GROUP BY projcd, spjangcd
        ),
        accin_sum AS (
            SELECT
                projno,
                spjangcd,
                SUM(COALESCE(accin, 0)) AS total_accin
            FROM tb_banktransit
            WHERE ioflag = '0'
            GROUP BY projno, spjangcd
        ),
        accout_sum AS (
            SELECT
                projno,
                spjangcd,
                SUM(COALESCE(accout, 0)) AS total_accout
            FROM tb_banktransit
            WHERE ioflag = '1'
            GROUP BY projno, spjangcd
        )
        SELECT
            da003.projno,
            da003.projnm,
            COALESCE(suju.suju_totalamt, 0) AS suju_totalamt,
            COALESCE(sales.sales_totalamt, 0) AS sales_totalamt,
            COALESCE(invo.invo_totalamt, 0) AS invo_totalamt,
            COALESCE(accin.total_accin, 0) AS total_accin,
            COALESCE(accout.total_accout, 0) AS total_accout,
            TO_CHAR(TO_DATE(da003.contdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS contdate
        FROM tb_da003 da003
        LEFT JOIN suju_sum suju 
            ON suju.project_id = da003.projno AND suju.spjangcd = da003.spjangcd
        LEFT JOIN sales_sum sales 
            ON sales.projectcode = da003.projno AND sales.spjangcd = da003.spjangcd
        LEFT JOIN invo_sum invo 
            ON invo.projcd = da003.projno AND invo.spjangcd = da003.spjangcd
        LEFT JOIN accin_sum accin 
            ON accin.projno = da003.projno AND accin.spjangcd = da003.spjangcd
        LEFT JOIN accout_sum accout 
            ON accout.projno = da003.projno AND accout.spjangcd = da003.spjangcd
        WHERE da003.spjangcd = :spjangcd
        """;

    // 조건: 프로젝트명 필터
    if (txtProjectName != null && !txtProjectName.isEmpty()) {
      sql += " AND da003.projnm LIKE :txtDescription ";
      dicParam.addValue("txtDescription", "%" + txtProjectName + "%");
    }

    // 조건: 계약연도 필터
    if (cboYear != null && !cboYear.isEmpty()) {
      sql += " AND da003.contdate LIKE :cboYear ";
      dicParam.addValue("cboYear", cboYear + "%");
    }

    sql += " ORDER BY accin.projno ";

//    log.info("프로젝트 현황 AllRead SQL: {}", sql);
//    log.info("SQL Parameters: {}", dicParam.getValues());

    return this.sqlRunner.getRows(sql, dicParam);
  }

  //경비 사용내역
  public List<Map<String, Object>> getExpenseHistory(String spjangcd, String projno) {
    MapSqlParameterSource dicParam = new MapSqlParameterSource();
    dicParam.addValue("spjangcd", spjangcd);
    dicParam.addValue("projno", projno);

    String sql = """
        select
        to_char(to_date(i.misdate, 'YYYYMMDD'), 'YYYY-MM-DD') as misdate,
        i.misnum ,
        i.misseq,
        i.artcd ,
        ca648.artnm ,
        i.itemnm,
        i.spec,
        i.qty,
        i.unitcost,
        i.supplycost,
        i.taxtotal,
        i.remark
        from tb_invoicedetail i
        left join tb_ca648 ca648 on i.artcd = ca648.artcd
        where i.spjangcd =:spjangcd 
         and i.projcd =:projno
          order by i.misdate,i.misnum, i.misseq
        """;

    List<Map<String, Object>> itmes = this.sqlRunner.getRows(sql, dicParam);

    return itmes;
  }

  //매출내역
  public List<Map<String, Object>> getSalesHistory(String spjangcd, String projno) {

    MapSqlParameterSource dicParam = new MapSqlParameterSource();
    dicParam.addValue("spjangcd", spjangcd);
    dicParam.addValue("projno", projno);

    String sql = """
        select
         to_char(to_date(s.misdate, 'YYYYMMDD'), 'YYYY-MM-DD') as misdate,
         cs."Value" as misgubun,
          sc5."Name" AS "icerdeptnm",
         s.misnum ,
         d.misseq ,
         d.itemnm,
         d.spec,
         d.qty,
         d.unitcost,
         d.supplycost,
         d.taxtotal,
         d.totalamt,
         d.remark
         from tb_salesment s
         left join tb_salesdetail d on s.misnum =d.misnum and s.spjangcd =d.spjangcd
         left join sys_code cs on cs."Code" = s.misgubun
         LEFT JOIN depart sc5 ON sc5."Code" = s.departcode
         where s.spjangcd = :spjangcd
         and s.projectcode =:projno
         ORDER BY s.misdate
        """;

//    log.info("프로젝트 현황_매출내역 SQL: {}", sql);
//    log.info("SQL Parameters: {}", dicParam.getValues());
    List<Map<String, Object>> itmes = this.sqlRunner.getRows(sql, dicParam);

    return itmes;
  }

  //입출금내역
  public List<Map<String, Object>> getTransactionHistory(String spjangcd, String projno) {
    MapSqlParameterSource dicParam = new MapSqlParameterSource();
    dicParam.addValue("spjangcd", spjangcd);
    dicParam.addValue("projno", projno);
    String sql = """
       SELECT 
         TO_CHAR(TO_DATE(b.trdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS trdate,
         c."Name" AS comp_name,
         CASE b.ioflag
           WHEN '0' THEN '입금'
           WHEN '1' THEN '출금'
         END AS io_type,
         b.accin ,
         b.accout ,
         b.memo
       FROM tb_banktransit b
       LEFT JOIN company c ON b.cltcd = c.id 
       WHERE b.spjangcd = :spjangcd
       AND b.projno =:projno
        ORDER BY b.trdate
        """;
//    log.info("프로젝트 현황_입출금내역 SQL: {}", sql);
//    log.info("SQL Parameters: {}", dicParam.getValues());
    List<Map<String, Object>> itmes = this.sqlRunner.getRows(sql, dicParam);

    return itmes;
  }
}
