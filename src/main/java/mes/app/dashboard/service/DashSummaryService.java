package mes.app.dashboard.service;

import io.micrometer.core.instrument.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import mes.app.common.TenantContext;
import mes.domain.services.DateUtil;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class DashSummaryService {

    @Autowired
    SqlRunner sqlRunner;

    public List<Map<String, Object>> getOrderStatusByOperid(String startDate, String endDate, String perid, String spjangcd, String searchCltnm, String searchtketnm, String searchstate) {

        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("perid", perid);
        params.addValue("spjangcd", spjangcd);

        if (startDate != null && !startDate.isEmpty()) {
            params.addValue("startDate", startDate);
        }
        if (endDate != null && !endDate.isEmpty()) {
            params.addValue("endDate", endDate);
        }

        StringBuilder sql = new StringBuilder("""
       SELECT
          tb006.*,
          uc.Value AS ordflag_display,
          (
              SELECT ISNULL((
                  SELECT
                      bd.filepath,
                      bd.filesvnm,
                      bd.fileextns,
                      bd.fileurl,
                      bd.fileornm,
                      bd.filesize,
                      bd.fileid
                  FROM
                      tb_DA006WFILE bd
                  WHERE
                      bd.custcd = tb006.custcd
                      AND bd.spjangcd = tb006.spjangcd
                      AND bd.reqdate = tb006.reqdate
                      AND bd.reqnum = tb006.reqnum
                  ORDER BY
                      bd.indatem DESC
                  FOR JSON PATH
              ), '[]')
          ) AS hd_files
      FROM
          TB_DA006W tb006 
      left join user_code uc on uc.Code = tb006.ordflag
      WHERE
          tb006.spjangcd = :spjangcd
    """);

        // 날짜 필터링 (TB_DA006W 기준)
        if (startDate != null && !startDate.isEmpty()) {
            startDate = startDate.replace("-", ""); // "2025-03-01" -> "20250301"
            sql.append(" and tb006.reqdate >= :startDate ");
            params.addValue("startDate", startDate );
        }
        if (endDate != null && !endDate.isEmpty()) {
            sql.append("  AND tb006.reqdate <= :endDate ");
            params.addValue("endDate", endDate);
        }

        // 검색 조건 추가 (TB_DA006W 기준)
        if (searchCltnm != null && !searchCltnm.isEmpty()) {
            sql.append(" AND tb006.cltnm LIKE :searchCltnm ");
            params.addValue("searchCltnm", "%" + searchCltnm + "%"); //`%` 추가하여 LIKE 검색 가능하도록 변경
        }
        if (searchtketnm != null && !searchtketnm.isEmpty()) {
            sql.append(" AND tb006.remark LIKE :searchtketnm ");
            params.addValue("searchtketnm", "%" + searchtketnm + "%");
        }

        // "전체"일 경우 조건을 추가하지 않음
        if (searchstate != null && !searchstate.equals("all") && !searchstate.isEmpty()) {
            sql.append(" AND tb006.ordflag = :searchstate ");
            params.addValue("searchstate", searchstate);
        }

        // 정렬 조건 추가
        sql.append(" ORDER BY tb006.reqdate DESC");

        //log.info(" 실행될 SQL: {}", sql);
        //log.info("바인딩된 파라미터: {}", params.getValues());

        return sqlRunner.getRows(sql.toString(), params);
    }

    // 업무일지 조회
    public List<Map<String, Object>> getList(String start, String end, String searchPernm, String spjangcd) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("start", start);
        dicParam.addValue("end", end);
        dicParam.addValue("searchPernm", searchPernm);
        dicParam.addValue("spjangcd", spjangcd);

        String sql = """
                SELECT
                    a."rptid" AS id,
                    TO_CHAR(TO_DATE(a.rptdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS rptdate,
                    a."rptweek",
                    a."frdate",
                    a."todate",
                    a."fixperid",
                    a."fixpernm",
                    a."cltnm",

                    a."fixflag",
                    sc1."Value" AS fixflag_nm,

                    a."asmenu",

                    a."actflag",
                    sc2."Value" AS actflag_nm,

                    a."asdv",
                    sc3."Value" AS asdv_nm,

                    a."recyn",
                    sc4."Value" AS recyn_nm,

                    a."rptremark",
                    a."etcremark",
                    a."remark",
                    TO_CHAR(a."inputdate", 'YYYY-MM-DD HH24:MI') AS inputdate

                FROM "tb_as020" a

                LEFT JOIN "sys_code" sc1
                    ON sc1."Code" = a."fixflag"
                   AND sc1."CodeType" = 'fixflag'

                LEFT JOIN "sys_code" sc2
                    ON sc2."Code" = a."actflag"
                   AND sc2."CodeType" = 'actflag'

                LEFT JOIN "sys_code" sc3
                    ON sc3."Code" = a."asdv"
                   AND sc3."CodeType" = 'asdv'

                LEFT JOIN "sys_code" sc4
                    ON sc4."Code" = a."recyn"
                   AND sc4."CodeType" = 'recyn'
                WHERE 1=1
                  AND a.rptdate BETWEEN :start AND :end
            """;

        // searchPernm 이 있을 때만 조건 추가
        if (searchPernm != null && !searchPernm.trim().isEmpty()) {
            sql += " AND a.fixpernm LIKE CONCAT('%', :searchPernm, '%') ";
        }

        sql += " ORDER BY fixflag ASC, rptdate DESC ";

        return this.sqlRunner.getRows(sql, dicParam);
    }

    public List<Map<String, Object>> getModalListByClientName(String searchTerm) {
        MapSqlParameterSource params = new MapSqlParameterSource();

        // searchTerm이 있을 때만 LIKE 조건 추가
        String sql = """
        SELECT *
                FROM TB_XCLIENT
        """ + (searchTerm != null && !searchTerm.isEmpty() ? " WHERE cltnm LIKE :searchTerm" : "");

        // searchTerm이 비어 있지 않을 때만 파라미터에 추가
        if (searchTerm != null && !searchTerm.isEmpty()) {
            params.addValue("searchTerm", "%" + searchTerm + "%");
        }
        return sqlRunner.getRows(sql, params);
    }

    public List<Map<String, Object>> searchData(String startDate, String endDate, String searchCltnm, String searchtketnm, String searchstate) {

        MapSqlParameterSource params = new MapSqlParameterSource();
        if (startDate != null && !startDate.isEmpty()) {
            params.addValue("startDate", startDate);
        }
        if (endDate != null && !endDate.isEmpty()) {
            params.addValue("endDate", endDate);
        }
        if (searchCltnm != null && !searchCltnm.isEmpty()) {
            params.addValue("searchCltnm", "%" + searchCltnm + "%");
        }
        if (searchtketnm != null && !searchtketnm.isEmpty()) {
            params.addValue("searchtketnm", "%" + searchtketnm + "%");
        }
        if (searchstate != null && !searchstate.isEmpty() && !searchstate.equals("전체")) {
            params.addValue("searchstate", searchstate);
        }

        // 기본 SQL 쿼리 작성
        String sql = """
        SELECT
            tb007.*,
            tb007.reqdate,
            tb006.*,
            tb006.cltnm,
            tb006.remark,
            tb006.ordflag,
            (
                   SELECT
                       bd.filepath,
                       bd.filesvnm,
                       bd.fileextns,
                       bd.fileurl,
                       bd.fileornm,
                       bd.filesize,
                       bd.fileid
                   FROM
                       tb_DA006WFILE bd
                   WHERE
                       bd.custcd = tb007.custcd
                       AND bd.spjangcd = tb007.spjangcd
                       AND bd.reqdate = tb007.reqdate
                       AND bd.reqnum = tb007.reqnum
                   ORDER BY
                       bd.indatem DESC
                   FOR JSON PATH
               ) AS hd_files
        FROM
            TB_DA007W tb007
        LEFT JOIN
            TB_DA006W tb006
        ON
            tb007.custcd = tb006.custcd
            AND tb007.spjangcd = tb006.spjangcd
            AND tb007.reqdate = tb006.reqdate
            AND tb007.reqnum = tb006.reqnum
        WHERE 1=1
    """;

        // 조건 추가
        if (params.hasValue("startDate")) {
            sql += " AND tb007.reqdate >= :startDate";
        }
        if (params.hasValue("endDate")) {
            sql += " AND tb007.reqdate <= :endDate";
        }
        if (params.hasValue("searchCltnm")) {
            sql += " AND tb006.cltnm LIKE :searchCltnm";
        }
        if (params.hasValue("searchtketnm")) {
            sql += " AND tb006.remark LIKE :searchtketnm";
        }
        if (params.hasValue("searchstate")) {
            sql += " AND tb006.ordflag = :searchstate";
        }

        // 쿼리 실행 및 결과 반환
//        log.info(" 실행될 SQL: {}", sql);
//        log.info("바인딩된 파라미터: {}", params.getValues());
        return sqlRunner.getRows(sql, params);
    }

    public String getOrdtextByParams(String reqdate, String remark) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("reqdate", reqdate);
        params.addValue("remark", remark);

        // ordtext를 가져오는 SQL 쿼리 작성
        String sql = """  
        SELECT
            tb007.ordtext,
            tb007.*,
            tb006.*
        FROM
            TB_DA007W tb007
        JOIN
            TB_DA006W tb006
        ON
            tb007.custcd = tb006.custcd
            AND tb007.spjangcd = tb006.spjangcd
            AND tb007.reqdate = tb006.reqdate
            AND tb007.reqnum = tb006.reqnum
        WHERE
            tb006.reqdate = :reqdate
            AND tb006.remark = :remark
        """;

        // 쿼리 실행 및 결과 반환
        List<Map<String, Object>> result = sqlRunner.getRows(sql, params);

        // 결과에서 ordtext 값을 추출
        if (!result.isEmpty() && result.get(0).get("ordtext") != null) {
            return result.get(0).get("ordtext").toString();
        }
        return null; // 데이터가 없을 경우 null 반환
    }

    // username으로 cltcd, cltnm, saupnum, custcd 가지고 오기
    public Map<String, Object> getUserInfo(String username) {
        MapSqlParameterSource dicParam = new MapSqlParameterSource();

        String sql = """
                select xc.custcd,
                       xc.cltcd,
                       xc.cltnm,
                       xc.saupnum,
                       au.spjangcd
                FROM TB_XCLIENT xc
                left join auth_user au on au."username" = xc.saupnum
                WHERE xc.saupnum = :username
                """;
        dicParam.addValue("username", username);
        Map<String, Object> userInfo = this.sqlRunner.getRow(sql, dicParam);
        return userInfo;
    }

    //근태현황 그리드
    public List<Map<String, Object>> getOrderList(String spjangcd, String searchType) {
        MapSqlParameterSource paramMap = new MapSqlParameterSource();

        paramMap.addValue("spjangcd", spjangcd);

        StringBuilder sql = new StringBuilder("""
      SELECT
          t.id as id,
          t.spjangcd as spjangcd,
          t.reqdate as reqdate,
          t.personid as personid,
          t.frdate as frdate,
          t.todate as todate,
          t.sttime as sttime,
          t.edtime as edtime,
          t.daynum as daynum,
          t.workcd as workcd,
          t.remark as remark,
          t.fixflag as fixflag,
          tb210.yearflag as yearflag,
          tb210.worknm as worknm,
          p."Name" as first_name,
          s."Value" as jik_id,
          sc."Value" as appgubunnm
      FROM tb_pb204 t
          LEFT JOIN person p ON p.id = t.personid
          LEFT JOIN (
              SELECT "Code", "Value"
              FROM sys_code
              WHERE "CodeType" = 'jik_type'
          ) s ON s."Code" = p.jik_id
          LEFT JOIN (
              SELECT "Code", "Value"
              FROM sys_code
              WHERE "CodeType" = 'approval_status'
          ) sc ON sc."Code" = t.appgubun
          LEFT JOIN tb_pb210 tb210 ON tb210.workcd = t.workcd
      WHERE
          t.spjangcd = :spjangcd
          AND TO_DATE(t.frdate, 'YYYYMMDD') BETWEEN
              date_trunc('month', CURRENT_DATE)::date
            AND (date_trunc('month', CURRENT_DATE) + INTERVAL '1 month - 1 day')::date
    """);

        // 진행구분 필터
        if (searchType != null && !searchType.isEmpty()) {
            paramMap.addValue("searchType", searchType);
            sql.append(" AND t.workcd = :searchType ");
        }
        sql.append(" ORDER BY t.reqdate ");

        List<Map<String, Object>> items = this.sqlRunner.getRows(String.valueOf(sql), paramMap);
        return items;
    }

    public List<Map<String, Object>> initDatas(String searchSpjangcd) {
        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        StringBuilder sql = new StringBuilder("""
                SELECT
                    hd.workcd,
                    COUNT(*) AS workcd_count
                FROM
                    tb_pb204 hd
                WHERE
                    hd.spjangcd = :spjangcd
                    AND TO_DATE(hd.frdate, 'YYYYMMDD') >= date_trunc('month', CURRENT_DATE)
                    AND TO_DATE(hd.frdate, 'YYYYMMDD') < (date_trunc('month', CURRENT_DATE) + INTERVAL '1 month')
                GROUP BY
                    hd.workcd;
                """);
        dicParam.addValue("spjangcd", searchSpjangcd);
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql.toString(), dicParam);
        return items;
    }

    // 근태현황 캘린더
    public List<Map<String, Object>> getOrderList2() {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();

        String tenantId = TenantContext.get();
        dicParam.addValue("spjangcd", tenantId);

        StringBuilder sql = new StringBuilder("""
                SELECT
                    tb204.*,
                    tb210.worknm as worknm,
                    per."Name"
                FROM
                    tb_pb204 tb204
                    LEFT JOIN tb_pb210 tb210 ON tb210.workcd = tb204.workcd
                    LEFT JOIN person per ON tb204.personid = per.id
                WHERE
                    TO_DATE(tb204.frdate, 'YYYYMMDD') BETWEEN
                        TO_DATE((EXTRACT(YEAR FROM CURRENT_DATE) - 1)::text || '0101', 'YYYYMMDD')
                        AND TO_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::text || '1231', 'YYYYMMDD')
                        and tb204.spjangcd = :spjangcd
                """);
        // 정렬 조건 추가
        sql.append(" ORDER BY tb204.frdate ASC");

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql.toString(), dicParam);
        return items;
    }
//
//
//    public TB_DA006W UpdateOrdflag(List<Map<String, Object>> orders) {
//        for (Map<String, Object> order : orders) {
//            String reqnum = (String) order.get("reqnum"); // 주문 번호
//            String ordflag = (String) order.get("ordflag"); // 0 또는 1 (문자열)
//
//            // "0" → "1", "1" → "0" 변환 후 업데이트
//            String newOrdflag = "0".equals(ordflag) ? "1" : "0";
//
//            String sql = """
//            UPDATE TB_DA006W
//            SET ordflag = :ordflag
//            WHERE reqnum = :reqnum
//        """;
//
//            MapSqlParameterSource params = new MapSqlParameterSource();
//            params.addValue("ordflag", newOrdflag);
//            params.addValue("reqnum", reqnum);
//
////            log.info("📌 주문 상태 변경 SQL 실행: {}", sql);
////            log.info("📌 SQL Parameters: {}", params.getValues());
//
//            sqlRunner.execute(sql, params);
//        }
//
//        return new TB_DA006W(); // 업데이트 결과 반환 (실제 로직에 맞게 수정 필요)
//    }
//
//    public int CancelOrderUpdateOrdflag(List<Map<String, Object>> orders) {
//        int updatedCount = 0;
//        for (Map<String, Object> order : orders) {
//            String reqnum = (String) order.get("reqnum"); // 주문 번호
//
//            String sql = """
//        UPDATE TB_DA006W
//        SET ordflag = :ordflag
//        WHERE reqnum = :reqnum
//        """;
//
//            MapSqlParameterSource params = new MapSqlParameterSource();
//            params.addValue("ordflag", "5"); // 컨트롤러에서 "5"로 변환했으므로 그대로 저장
//            params.addValue("reqnum", reqnum);
//
//            // SQL 실행 로그 추가
////            log.info("📌 실행할 SQL: {}", sql);
////            log.info("📌 SQL 파라미터: {}", params.getValues());
//
//            // SQL 실행 및 변경된 행 수 확인
//            int result = sqlRunner.execute(sql, params);
//            updatedCount += result;
//        }
//
//        // 업데이트된 행 수 반환
//        return updatedCount;
//    }

    // 요청사항 조회 (처리 대기 목록)
    public List<Map<String, Object>> searchDatas4(
            String searchfrdate
            , String searchtodate
            , String searchCompCd
            , String searchCompnm
            , String reqType
            , String spjangcd
            , Integer perId
            , String recyn
            , String aspernm
    ) {
        // 날짜 형식 변환 (YYYY-MM-DD -> YYYYMMDD)
        if (searchfrdate != null && searchfrdate.contains("-")) {
            searchfrdate = searchfrdate.replaceAll("-", "");
        }
        if (searchtodate != null && searchtodate.contains("-")) {
            searchtodate = searchtodate.replaceAll("-", "");
        }

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("searchfrdate", searchfrdate);
        dicParam.addValue("searchtodate", searchtodate);
        dicParam.addValue("searchCompCd", searchCompCd);
        dicParam.addValue("reqType", reqType);
        dicParam.addValue("spjangcd", spjangcd);

        String sql = """
                SELECT
                    a."asid" AS id,
                    TO_CHAR(TO_DATE(a."asdate", 'YYYYMMDD'), 'YYYY-MM-DD') AS asdate,
                    a."cltnm",
                    a."cltcd",
                    a."userid",
                    a."usernm",
                    a."asperid",
                    a."aspernm",
                    a."retitle",
                    a."remark" AS content,
                    a."asdv",
                    sc1."Value" AS asdv_nm,
                    a."asmenu",
                    a."recyn",
                    sc2."Value" AS recyn_nm,
                    a."recperid",
                    a."recpernm",
                    TO_CHAR(a."recdate", 'YYYY-MM-DD HH24:MI') AS recdate,
                    a."endperid",
                    a."endpernm",
                    a."as_file",
                    f."as_file" as "fix_file",
                    TO_CHAR(TO_DATE(a."enddate", 'YYYYMMDD'), 'YYYY-MM-DD') AS enddate,
                    TO_CHAR(a."inputdate", 'YYYY-MM-DD HH24:MI') AS inputdate,
                    CASE WHEN f."fixid" IS NOT NULL THEN 'Y' ELSE 'N' END AS hasProcess
                FROM "tb_as010" a
                LEFT JOIN "sys_code" sc1
                    ON sc1."Code" = a."asdv"
                   AND sc1."CodeType" = 'asdv'
                LEFT JOIN "sys_code" sc2
                    ON sc2."Code" = a."recyn"
                   AND sc2."CodeType" = 'recyn'
                LEFT JOIN "tb_as011" f
                    ON f."asid" = a."asid"
                WHERE 1=1
        		""";

        // 날짜 조건 추가
        if (searchfrdate != null && !searchfrdate.isEmpty()) {
            sql += " AND a.\"asdate\" >= :searchfrdate ";
        }
        if (searchCompnm != null && !searchCompnm.isEmpty()) { // 본사담당 검색필터
            dicParam.addValue("searchCompnm", '%' + searchCompnm.toString() + '%');
            sql += " AND a.\"cltnm\" like :searchCompnm ";
        }
        if (aspernm != null && !aspernm.isEmpty()) { // 본사담당 검색필터, 담당자 배정받지 않은 건도 표시
            dicParam.addValue("aspernm", aspernm.toString());
            dicParam.addValue("asperid", perId);
            sql += " AND (a.\"aspernm\" = :aspernm OR a.\"aspernm\" IS NULL OR a.\"asperid\" = :asperid ) ";
        }
        if (searchtodate != null && !searchtodate.isEmpty()) {
            sql += " AND a.\"asdate\" <= :searchtodate ";
        }

        // 업체 조건 추가
        if (searchCompCd != null && !searchCompCd.isEmpty()) {
            sql += " AND a.\"cltcd\" = :searchCompCd ";
        }

        // 요청구분 조건 추가
        if (reqType != null && !reqType.isEmpty()) {
            sql += " AND a.\"asdv\" = :reqType ";
        }
        // 진행구분 조건 추가
        if (recyn != null && !recyn.isEmpty()) {
            dicParam.addValue("recyn", recyn);
            sql += " AND a.\"recyn\" = :recyn ";
        }

        sql += " ORDER BY a.\"asdate\" DESC, a.\"inputdate\" DESC ";

        List<Map<String, Object>> item = this.sqlRunner.getRows(sql, dicParam);

        return item;
    }


    // 요청사항 조회 (처리 대기 목록)
    public List<Map<String, Object>> searchDatas2(
//            String searchfrdate
//            , String searchtodate
//            , String searchCompCd
//            , String searchCompnm
//            , String reqType
//            , String spjangcd
//            , Integer perId
//            , String recyn
//            , String aspernm
    ) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();

        String sql = """
                SELECT
                    a."asid" AS id,
                    TO_CHAR(TO_DATE(a."asdate", 'YYYYMMDD'), 'YYYY-MM-DD') AS asdate,
                    a."cltnm",
                    a."cltcd",
                    a."userid",
                    a."usernm",
                    a."asperid",
                    a."aspernm",
                    a."retitle",
                    a."remark" AS content,
                    a."asdv",
                    sc1."Value" AS asdv_nm,
                    a."asmenu",
                    a."recyn",
                    sc2."Value" AS recyn_nm,
                    a."recperid",
                    a."recpernm",
                    TO_CHAR(a."recdate", 'YYYY-MM-DD HH24:MI') AS recdate,
                    a."endperid",
                    a."endpernm",
                    a."as_file",
                    f."as_file" as "fix_file",
                    TO_CHAR(TO_DATE(a."enddate", 'YYYYMMDD'), 'YYYY-MM-DD') AS enddate,
                    TO_CHAR(a."inputdate", 'YYYY-MM-DD HH24:MI') AS inputdate,
                    CASE WHEN f."fixid" IS NOT NULL THEN 'Y' ELSE 'N' END AS hasProcess
                FROM "tb_as010" a
                LEFT JOIN "sys_code" sc1
                    ON sc1."Code" = a."asdv"
                   AND sc1."CodeType" = 'asdv'
                LEFT JOIN "sys_code" sc2
                    ON sc2."Code" = a."recyn"
                   AND sc2."CodeType" = 'recyn'
                LEFT JOIN "tb_as011" f
                    ON f."asid" = a."asid"
                WHERE 1=1
                AND TO_DATE(a."asdate", 'YYYYMMDD')::date
                    BETWEEN date_trunc('month', CURRENT_DATE)
                        AND (date_trunc('month', CURRENT_DATE) + INTERVAL '1 month - 1 day')
        		""";



        sql += " ORDER BY a.\"asdate\" DESC, a.\"inputdate\" DESC ";

        List<Map<String, Object>> item = this.sqlRunner.getRows(sql, dicParam);

        return item;
    }

    // 공지사항 조회
    public List<Map<String, Object>> getBoardList(String board_group, String keyword, String srchStartDt, String srchEndDt) {

        String today = DateUtil.getTodayString();
        String tenantId = TenantContext.get();
        MapSqlParameterSource paramMap = new MapSqlParameterSource();
        paramMap.addValue("board_group", board_group);
        paramMap.addValue("srchStartDt", Timestamp.valueOf(srchStartDt));
        paramMap.addValue("srchEndDt", Timestamp.valueOf(srchEndDt));
        paramMap.addValue("keyword", keyword);
        paramMap.addValue("today", Date.valueOf(today));
        paramMap.addValue("spjangcd", tenantId);

        String sql = """
        		with A as (
                    select id, "Title" as title
	                , to_char("WriteDateTime", 'yyyy-mm-dd hh24:mi:ss') as write_date_time
	                , "Content" as content
	                , "NoticeEndDate" AS notice_end_date
                    , "NoticeYN" AS notice_yn
	                from board
	                where "BoardGroup" = :board_group
                    and "NoticeYN" = 'Y'
	                and "NoticeEndDate" >= :today
	                and spjangcd = :spjangcd
                ), B as (
                    select B.id, B."Title" as title
                    , to_char(B."WriteDateTime", 'yyyy-mm-dd hh24:mi:ss') as write_date_time
                    , "Content" as content
                    , "NoticeEndDate" AS notice_end_date
                    , "NoticeYN" AS notice_yn
                    from board B 
                    left join A on A.id = B.id
                    where B."BoardGroup" = :board_group
                    and B."WriteDateTime" between :srchStartDt and :srchEndDt
                    and A.id is null
                    and B.spjangcd = :spjangcd
        		     """;

        if (StringUtils.isEmpty(keyword) == false) {
            sql += """
        			and ( B."Title" like concat('%%', :keyword, '%%') 
                        or B."Content" like concat('%%', :keyword, '%%')
                        )
        			""";
        }

        sql += """
        		)
            select 1 as data_group
            , id
            , title
            , write_date_time
            , content
            , notice_end_date
            , notice_yn
            from A 
            union all 
            select 2 as data_group
            , id
            , title
            , write_date_time
            , content
            , notice_end_date
            , notice_yn
            from B 
            order by data_group, write_date_time desc
        		""";

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

        return items;
    }
}