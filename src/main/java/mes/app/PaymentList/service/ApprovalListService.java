package mes.app.PaymentList.service;

import lombok.extern.slf4j.Slf4j;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class ApprovalListService {//결재목록

  @Autowired
  SqlRunner sqlRunner;

  public List<Map<String, Object>> getPaymentList(String spjangcd , String startDate, String endDate, String searchPayment, String searchUserNm, Integer personid) {
    MapSqlParameterSource params = new MapSqlParameterSource();
    params.addValue("as_spjangcd", spjangcd);
    params.addValue("personid", personid);
    StringBuilder sql = new StringBuilder("""
      SELECT
                     e080.repodate,
                     e080.repoperid,
                     (SELECT "Name" FROM person WHERE id = repoperid) AS repopernm,
                     -- ca510.com_code AS papercd,
                     -- ca510.com_cnam AS papercd_name,
                     e080.appgubun,
                     uc."Value" AS appgubun_display,
                     e080.repodate,
                     e080.appnum,
                     e080.personid,
                     e080.title,
                     e080.indate,
                     ps."Value" as papercd_name,
                     ac."Value" as appgubun_display,
                     tb204.remark
                     -- 파일 추가될 경우 주석부분 추가작업 필요
         --          CASE
         --               WHEN EXISTS (
         --                    SELECT 1 FROM TB_AA010ATCH
         --                    WHERE spdate = ('A' || e080.appnum) OR spdate = ('AS' || e080.appnum)
         --                ) THEN (
         --                    SELECT CONCAT(spdate, '|', filename, '|', filepath)
         --                    FROM TB_AA010ATCH
         --                    WHERE spdate = ('A' || e080.appnum) OR spdate = ('AS' || e080.appnum)
         --                    LIMIT 1
         --                )
         --                ELSE (
         --                    SELECT CONCAT(spdate, '|', filename, '|', filepath)
         --                    FROM TB_AA010PDF
         --                    WHERE spdate = e080.appnum
         --                    LIMIT 1
         --                )
         --           END AS file_info
                FROM tb_e080 e080
                LEFT JOIN user_code uc ON uc."Code"= e080.appgubun
                LEFT JOIN sys_code ps ON ps."Code" = e080.papercd AND ps."CodeType" = 'appr_doc'
                LEFT JOIN sys_code ac ON ac."Code" = e080.appgubun AND ac."CodeType" = 'approval_status'
                LEFT JOIN tb_pb204 tb204 ON e080.appnum = tb204.appnum
                -- LEFT JOIN tb_ca510 ca510
                --     ON ca510.com_cls = '620'
                --    AND ca510.com_code = e080.papercd
                WHERE e080.spjangcd = :as_spjangcd
                  AND e080.repoperid = :personid
                 AND e080.flag = '1'
    """);

    // startDate 필터링
    if (startDate != null && !startDate.isEmpty()) {
      sql.append(" AND indate >= :as_stdate ");
      params.addValue("as_stdate", startDate);
    }

    // endDate 필터링
    if (endDate != null && !endDate.isEmpty()) {
      sql.append(" AND indate <= :as_enddate ");
      params.addValue("as_enddate", endDate);
    }

    // 검색 조건 추가
    if (searchUserNm != null && !searchUserNm.isEmpty()) {
      sql.append(" AND appperid LIKE :searchUserNm ");
      params.addValue("searchUserNm", "%" + searchUserNm + "%");
    }

    if (searchPayment != null && !searchPayment.isEmpty()) {
      sql.append(" AND appgubun = :as_appgubun ");
      params.addValue("as_appgubun", searchPayment);
    }

    sql.append(" ORDER BY indate DESC");

//    log.info("결재 목록 List SQL: {}", sql);
//    log.info("SQL Parameters: {}", params.getValues());
    return sqlRunner.getRows(sql.toString(), params);
  }


  // 사용자의 사업장코드 return
  public String getSpjangcd(String username
      , String searchSpjangcd) {
    MapSqlParameterSource dicParam = new MapSqlParameterSource();

    String sql = """
                SELECT spjangcd
                FROM auth_user
                WHERE username = :username
                """;
    dicParam.addValue("username", username);
    Map<String, Object> spjangcdMap = this.sqlRunner.getRow(sql, dicParam);
    String userSpjangcd = (String)spjangcdMap.get("spjangcd");

    String spjangcd = searchSpjangcd(searchSpjangcd, userSpjangcd);
    return spjangcd;
  }

  // init에 필요한 사업장코드 반환
  public String searchSpjangcd(String searchSpjangcd, String userSpjangcd){

    String resultSpjangcd = "";
    switch (searchSpjangcd){
      case "ZZ":
        resultSpjangcd = searchSpjangcd;
        break;
      case "PP":
        resultSpjangcd= searchSpjangcd;
        break;
      default:
        resultSpjangcd = userSpjangcd;
    }
    return resultSpjangcd;
  }

  public List<Map<String, Object>> getPaymentList1(String spjangcd, String startDate, String endDate, Integer personid) {
    MapSqlParameterSource params = new MapSqlParameterSource();
    params.addValue("as_spjangcd", spjangcd);
    params.addValue("as_stdate", startDate);
    params.addValue("as_enddate", endDate);
    params.addValue("as_perid", personid);
    StringBuilder sql = new StringBuilder("""
        SELECT (select count(appgubun) from tb_e080 where appgubun = '001' AND repoperid = :as_perid AND flag = '1' AND indate Between :as_stdate AND :as_enddate AND spjangcd = :as_spjangcd) as appgubun1,
               (select count(appgubun) from tb_e080 where appgubun = '101' AND repoperid = :as_perid AND flag = '1'  AND indate Between :as_stdate AND :as_enddate AND spjangcd = :as_spjangcd) as appgubun2,
               (select count(appgubun) from tb_e080 where appgubun = '131' AND repoperid = :as_perid AND flag = '1'  AND indate Between :as_stdate AND :as_enddate AND spjangcd = :as_spjangcd) as appgubun3,
               (select count(appgubun) from tb_e080 where appgubun = '201' AND repoperid = :as_perid AND flag = '1'  AND indate Between :as_stdate AND :as_enddate AND spjangcd = :as_spjangcd) as appgubun4
        """);
//    log.info("결재목록_문서현황 List SQL: {}", sql);
//    log.info("SQL Parameters: {}", params.getValues());
    return sqlRunner.getRows(sql.toString(), params);
  }

  public List<Map<String, Object>> getPaymentList2(String spjangcd, String appnum) {

    MapSqlParameterSource params = new MapSqlParameterSource();
    StringBuilder sql = new StringBuilder("""
         SELECT
            a.appnum,
            a.seq,
            a.personid,
            p."Name" as personnm,
            d."Name"as divinm,
            s."Value" as rspnm,
         --   (select pernm from tb_ja001 where perid='p' +  appperid) as apppernm,
         --      (select divinm from tb_jc002 where divicd=b.divicd and spjangcd=b.spjangcd) as divinm,
         --      (select rspnm from tb_pz001 where rspcd=b.rspcd and spjangcd=b.spjangcd) as rspnm,
         --   uc.Value AS appgubun_display,
            a.appgubun,
            sc."Value" appgubun_display,
            a.repodate
            FROM tb_e080 a
            LEFT JOIN user_code uc ON uc."Code" = a.appgubun
            LEFT JOIN person p ON p.id = a.personid
            LEFT JOIN depart d ON d.id = p."Depart_id"
            left join (
                            SELECT "Code", "Value"
                            FROM sys_code
                            WHERE "CodeType" = 'jik_type'
                    ) s on s."Code" = p.jik_id
            LEFT JOIN sys_code sc ON sc."Code" = a.appgubun AND sc."CodeType" = 'approval_status'
         --   JOIN tb_ja001 B on b.perid = 'p' + a.appperid and b.spjangcd=a.spjangcd
            AND  a.spjangcd = :as_spjangcd
            AND a.appnum = :as_appnum
            WHERE a.spjangcd = :as_spjangcd
                  AND a.appnum = :as_appnum
        """);
    params.addValue("as_spjangcd", spjangcd);
    params.addValue("as_appnum", appnum);

//    log.info("더블클릭 결재상세 SQL: {}", sql);
//    log.info("SQL Parameters: {}", params.getValues());
    return sqlRunner.getRows(sql.toString(), params);
  }

  public List<Map<String, Object>> getUserinfo(String spjangcd, String appnum) {
    MapSqlParameterSource params = new MapSqlParameterSource();
    StringBuilder sql = new StringBuilder("""
         SELECT  xu.custcd ,
                xu.userid ,
                xu.rnum   ,
                xu.passwd1,
                xu.passwd2,
                xu.custnm ,
                xu.pernm  ,
                xu.useyn  ,
                xu.perid  ,
                xu.spjangcd ,
         	(select divinm from tb_jc002 where divicd=b.divicd and spjangcd=b.spjangcd) as divinm,
           	(select rspnm from tb_pz001 where rspcd=b.rspcd and spjangcd=b.spjangcd) as rspnm
         from TB_XUSERS xu
         JOIN tb_ja001 B on b.perid = xu.perid and b.spjangcd= xu.spjangcd;
        """);
//    log.info("더블클릭 결재상세 SQL: {}", sql);
//    log.info("SQL Parameters: {}", params.getValues());
    return sqlRunner.getRows(sql.toString(), params);

  }
}
