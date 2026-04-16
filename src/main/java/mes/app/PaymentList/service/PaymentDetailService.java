package mes.app.PaymentList.service;

import lombok.extern.slf4j.Slf4j;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
public class PaymentDetailService {

  @Autowired
  SqlRunner sqlRunner;

  public List<Map<String, Object>> getPaymentList(String spjangcd, String startDate, String endDate, String searchPayment, String searchText, Integer personid) {
    MapSqlParameterSource params = new MapSqlParameterSource();
    params.addValue("as_spjangcd", spjangcd);
    params.addValue("personid", personid);
    StringBuilder sql = new StringBuilder("""
                SELECT
                     e080.repodate,
                     e080.repoperid,
                     (SELECT "Name" FROM person WHERE id = e080.repoperid) AS repopernm,
                     e080.appgubun,
                     e080.papercd,
                     scd."Value" as papercd_name,
                     -- ca510.com_code AS papercd,
                     -- ca510.com_cnam AS papercd_name,
                     sc."Value" AS appgubun_display,
                     -- e080.appdate,
                     e080.appnum,
                     e080.personid,
                     e080.title,
                     e080.indate,
                     pb204.remark
                     -- files.fileListJson
                 FROM tb_e080 e080
                 LEFT JOIN sys_code sc ON sc."Code" = e080.appgubun AND sc."CodeType" = 'approval_status'
                 LEFT JOIN sys_code scd ON scd."Code" = e080.papercd AND scd."CodeType" = 'appr_doc'
                 -- 휴가신청서 join
                 LEFT JOIN tb_pb204 pb204 ON e080.appnum = pb204.appnum
                 -- LEFT JOIN tb_ca510 ca510 ON ca510.com_cls = '620' AND ca510.com_code = e080.papercd
                 -- LEFT JOIN LATERAL (
                 -- SELECT json_agg(row_to_json(f)) AS fileListJson
                 -- FROM (
                 --   SELECT spdate, filename AS fileornm, filename AS filesvnm, filepath, '첨부' AS fileType
                 -- FROM TB_AA010ATCH
                 -- WHERE spdate IN ('A' || e080.appnum, 'AS' || e080.appnum, 'AJ' || e080.appnum)

                 -- UNION ALL

                 -- SELECT spdate, filename, filename, filepath, '전표'
                 --   FROM TB_AA010PDF
                 --   WHERE spdate = e080.appnum
                 -- ) f
                 -- ) files ON TRUE

                 WHERE e080.spjangcd = 'ZZ'
                  AND e080.personid = :personid
                  AND e080.flag = '1'
        """);
    // startDate 필터링
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
    String startDateFormatted = LocalDate.parse(startDate).format(formatter);
    sql.append(" AND e080.indate >= :as_stdate ");
    params.addValue("as_stdate", startDateFormatted);

    // endDate 필터링
    if (endDate != null && !endDate.isEmpty()) {
      String endDateFormatted = LocalDate.parse(endDate).format(formatter);
      sql.append(" AND e080.indate <= :as_enddate ");
      params.addValue("as_enddate", endDateFormatted);
    }

    // 검색 조건 추가
    if (searchText != null && !searchText.isEmpty()) {
      sql.append(" AND e080.title LIKE :searchText ");
      params.addValue("searchText", "%" + searchText + "%");
    }

    if (searchPayment == null || searchPayment.equals("all") || searchPayment.isEmpty()) {
      sql.append(" AND (e080.appgubun LIKE '%' OR :as_appgubun = '%') "); // 모든 값 허용
      params.addValue("as_appgubun", "%");
    } else {
      sql.append(" AND e080.appgubun = :as_appgubun ");
      params.addValue("as_appgubun", searchPayment);
    }
    sql.append(" ORDER BY e080.indate DESC ");

//    log.info("결재내역 List SQL: {}", sql);
//    log.info("SQL Parameters: {}", params.getValues());
    return sqlRunner.getRows(sql.toString(), params);

  }

  public Map<String, Object> getVacFileList(String appnum) {
    MapSqlParameterSource params = new MapSqlParameterSource();
    params.addValue("appnum", appnum);
    StringBuilder sql = new StringBuilder("""
                SELECT
                    e080.papercd,
                    s."Value" as papernm,
                    pb204.workcd,
                    pb210.worknm,
                    p."Name" as repopernm,
                    p.jik_id,
                    p."Depart_id",
                    sc."Value" as jiknm,
                    d."Name" as departnm,
                    pb204.remark,
                    pb204.frdate,
                    pb204.sttime,
                    pb204.todate,
                    pb204.edtime,
                    pb204.reqdate,
                    pb204.daynum
                 FROM tb_e080 e080
                 LEFT JOIN sys_code s ON s."Code" = e080.papercd AND s."CodeType" = 'appr_doc'
                 LEFT JOIN tb_pb204 pb204 ON pb204.appnum = e080.appnum
                 LEFT JOIN tb_pb210 pb210 ON pb210.workcd = pb204.workcd
                 LEFT JOIN person p ON p.id = e080.repoperid
                 LEFT JOIN sys_code sc ON sc."Code" = p.jik_id AND sc."CodeType" = 'jik_type'
                 LEFT JOIN depart d ON d.id = p."Depart_id"
                 WHERE e080.spjangcd = 'ZZ'
                 AND e080.appnum = :appnum
        """);
    return sqlRunner.getRow(sql.toString(), params);

  }


  public List<Map<String, Object>> getPaymentList1(String spjangcd, String startDate, String endDate, Integer personid) {
    MapSqlParameterSource params = new MapSqlParameterSource();

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
    String startDateFormatted = LocalDate.parse(startDate).format(formatter);
    String endDateFormatted = LocalDate.parse(endDate).format(formatter);

    params.addValue("as_stdate", startDateFormatted);
    params.addValue("as_enddate", endDateFormatted);
    params.addValue("as_spjangcd", spjangcd);
    params.addValue("as_personid", personid);
    StringBuilder sql = new StringBuilder("""
          SELECT
            (SELECT count(appgubun) FROM tb_e080 WHERE appgubun = '001' AND personid = :as_personid AND flag = '1' AND indate BETWEEN :as_stdate AND :as_enddate AND spjangcd = :as_spjangcd) AS appgubun1,
            (SELECT count(appgubun) FROM tb_e080 WHERE appgubun = '101' AND personid = :as_personid AND flag = '1' AND indate BETWEEN :as_stdate AND :as_enddate AND spjangcd = :as_spjangcd) AS appgubun2,
            (SELECT count(appgubun) FROM tb_e080 WHERE appgubun = '131' AND personid = :as_personid AND flag = '1' AND indate BETWEEN :as_stdate AND :as_enddate AND spjangcd = :as_spjangcd) AS appgubun3,
            (SELECT count(appgubun) FROM tb_e080 WHERE appgubun = '201' AND personid = :as_personid AND flag = '1' AND indate BETWEEN :as_stdate AND :as_enddate AND spjangcd = :as_spjangcd) AS appgubun4
        """);
//    log.info("결재목록_문서현황 List SQL: {}", sql);
//    log.info("SQL Parameters: {}", params.getValues());
    return sqlRunner.getRows(sql.toString(), params);
  }

  public Optional<String> findPdfFilenameByRealId(String appnum) {
    MapSqlParameterSource params = new MapSqlParameterSource();
    params.addValue("appnum", appnum);

    String sql = "select filename from TB_AA010PDF where spdate = :appnum;";

    try {
      // SQL 실행 후 결과 조회
//      log.info("결재승인PDF 파일 찾기 SQL: {}", sql);
//      log.info("SQL Parameters: {}", params.getValues());
      List<Map<String, Object>> result = sqlRunner.getRows(sql, params);

      if (!result.isEmpty() && result.get(0).get("filename") != null) {
        return Optional.of((String) result.get(0).get("filename"));
      }
    } catch (Exception e) {
      log.info("PDF 파일명을 조회하는 중 오류 발생: {}", e.getMessage(), e);
    }

    return Optional.empty(); // 결과가 없으면 빈 Optional 반환
  }

  public Optional<String> findPdfFilenameByRealId2(String appnum) {
    MapSqlParameterSource params = new MapSqlParameterSource();
    params.addValue("appnum", appnum);

    String sql = "select filename from TB_AA010ATCH WHERE spdate like 'A%' + :appnum;";

    try {
      // SQL 실행 후 결과 조회
//      log.info("첨부파일 PDF 파일 찾기 SQL: {}", sql);
//      log.info("SQL Parameters: {}", params.getValues());
      List<Map<String, Object>> result = sqlRunner.getRows(sql, params);

      if (!result.isEmpty() && result.get(0).get("filename") != null) {
        return Optional.of((String) result.get(0).get("filename"));
      }
    } catch (Exception e) {
      log.info("첨부파일 PDF 파일명을 조회하는 중 오류 발생: {}", e.getMessage(), e);
    }

    return Optional.empty(); // 결과가 없으면 빈 Optional 반환
  }

  // 지출결의서 (TB_AA007, TB_E080)
  public boolean updateStateForS(String appnum, String appgubun, String stateCode, String remark, Integer currentAppperid, String papercd) {
    MapSqlParameterSource params = new MapSqlParameterSource();
    params.addValue("appnum", appnum);

    // Step 1: TB_E080 결재라인 전체 조회
    String TB_E080Sql = """
            SELECT COUNT(*) AS cnt
            FROM TB_E080
            WHERE appnum = :appnum
              AND seq > (
                SELECT seq
                FROM TB_E080
                WHERE appnum = :appnum
                  AND personid = :currentAppperid
              )
              AND appgubun = '101'
        """;

    params.addValue("appnum", appnum);
    params.addValue("currentAppperid", currentAppperid);

    Map<String, Object> row = sqlRunner.getRow(TB_E080Sql, params);
    int count = row.get("cnt") != null ? ((Number) row.get("cnt")).intValue() : 0;

    // 상태 제한 처리
    if (count > 0 && !"101".equals(stateCode)) {
      log.warn("❌ 내 뒤에 있는 사람이 이미 승인함 → 승인 외 상태 변경 불가 (요청: {})", stateCode);
      return false;
    }

    log.info("✅ 상태 변경 가능: stateCode={}, 뒤에 승인자 수={}", stateCode, count);

    // Step 2: TB_AA007 문서 조회
    String aa007Sql = """
            SELECT *
            FROM TB_AA007
            WHERE appnum = :appnum
               OR 'S' + spdate + spnum + spjangcd = :appnum
        """;
    List<Map<String, Object>> aa007Rows = sqlRunner.getRows(aa007Sql, params);

    if (aa007Rows != null && !aa007Rows.isEmpty()) {
      log.info("✅ TB_AA007 문서 찾음: appnum={}", appnum);

      // 📌 remark 조건에 따라 동적 SQL 생성
      StringBuilder updateSql = new StringBuilder("""
        UPDATE TB_AA007
        SET appgubun = :action,
            inputdate = CURRENT_DATE
    """);

      if (remark != null && !remark.trim().isEmpty()) {
        updateSql.append(", remark = :remark");
        params.addValue("remark", remark);
      }

      updateSql.append("""
        WHERE appnum = :appnum
           OR 'S' + spdate + spnum + spjangcd = :appnum
    """);

      params.addValue("action", stateCode);
      int aa007Affected = sqlRunner.execute(updateSql.toString(), params);
      log.info("📝 TB_AA007 업데이트 완료: 변경된 row 수 = {}", aa007Affected);

    } else {
      log.warn("❌ TB_AA007에서 문서 찾지 못함: appnum={}", appnum);
      return false;
    }

// Step 3: TB_E080 업데이트 (현재 결재자만 대상)
    StringBuilder updateE080Sql = new StringBuilder("""
    UPDATE TB_E080
    SET appgubun = :action,
        remark = :remark,
""");

    if ("001".equals(stateCode)) {
      updateE080Sql.append("        appdate = NULL\n");
    } else {
      updateE080Sql.append("        appdate = TO_CHAR(CURRENT_DATE, 'YYYYMMDD')\n");
    }

    updateE080Sql.append("""
    WHERE appnum = :appnum
      AND personid = :currentAppperid
      AND papercd = :papercd
""");

    params.addValue("action", stateCode);
    params.addValue("remark", remark);
    params.addValue("currentAppperid", currentAppperid);
    params.addValue("papercd", papercd);

    int e080Affected = sqlRunner.execute(updateE080Sql.toString(), params);
    log.info("📝 TB_E080 상태 업데이트 완료: {}건", e080Affected);


    // Step 4: 상태코드에 따른 flag 처리
    if ("101".equals(stateCode) || "001".equals(stateCode)) {
      // 1. 현재 결재자 seq 가져오기
      String getSeqSql = """
      SELECT seq FROM TB_E080
      WHERE appnum = :appnum
        AND personid = :currentAppperid
  """;
      Object seqObj = sqlRunner.getRow(getSeqSql, params).get("seq");

      int currentSeq = 0;
      if (seqObj instanceof Number) {
        currentSeq = ((Number) seqObj).intValue();
      } else if (seqObj instanceof String) {
        currentSeq = Integer.parseInt((String) seqObj);
      }
      params.addValue("currentSeq", currentSeq);

      // 2. 다음 결재자 찾기
      String flagValue = "101".equals(stateCode) ? "0" : "1";

      String findNextSql = """
        SELECT seq FROM TB_E080
        WHERE appnum = :appnum
          AND seq > :currentSeq
          AND flag = :flag
        ORDER BY seq ASC
        LIMIT 1
      """;
      params.addValue("flag", flagValue);
      Map<String, Object> nextRow = sqlRunner.getRow(findNextSql, params);

      if (nextRow != null && nextRow.get("seq") != null) {
        Object nextSeqObj = nextRow.get("seq");
        int nextSeq = 0;
        if (nextSeqObj instanceof Number) {
          nextSeq = ((Number) nextSeqObj).intValue();
        } else if (nextSeqObj instanceof String) {
          nextSeq = Integer.parseInt((String) nextSeqObj);
        }

        String flagValue2 = "101".equals(stateCode) ? "1" : "0";

        String updateFlagSql = """
            UPDATE TB_E080
            SET flag = :flag
            WHERE appnum = :appnum
              AND seq = :nextSeq
        """;

        MapSqlParameterSource nextParams = new MapSqlParameterSource();
        nextParams.addValue("appnum", appnum);
        nextParams.addValue("nextSeq", nextSeq);
        nextParams.addValue("flag", flagValue2);

        int affected = sqlRunner.execute(updateFlagSql, nextParams);

        log.info("🔄 다음 결재자 flag = {} → 완료 (seq = {})",
            "101".equals(stateCode) ? "1" : "0", nextSeq);
      } else {
        log.info("📭 다음 결재자 없음 → 최종 승인자 또는 초기화 대상 없음");
      }
    }
    return e080Affected > 0;
  }


    // 전표문서 (TB_AA009, TB_E080)
  public boolean updateStateForNumberZZ(String appnum, String appgubun, String stateCode, String remark, Integer currentAppperid, String papercd) {
    MapSqlParameterSource params = new MapSqlParameterSource();
    params.addValue("appnum", appnum);

    // Step 1: TB_E080 결재라인 전체 조회
    String TB_E080Sql = """
    SELECT COUNT(*) AS cnt
    FROM TB_E080
    WHERE appnum = :appnum
      AND seq > (
        SELECT seq
        FROM TB_E080
        WHERE appnum = :appnum
          AND personid = :currentAppperid
      )
      AND appgubun = '101'
""";

    params.addValue("appnum", appnum);
    params.addValue("currentAppperid", currentAppperid);

    Map<String, Object> row = sqlRunner.getRow(TB_E080Sql, params);
    int count = row.get("cnt") != null ? ((Number) row.get("cnt")).intValue() : 0;

    // 상태 제한 처리
    if (count > 0 && !"101".equals(stateCode)) {
      log.warn("❌ 내 뒤에 있는 사람이 이미 승인함 → 승인 외 상태 변경 불가 (요청: {})", stateCode);
      return false;
    }

    log.info("✅ 상태 변경 가능: stateCode={}, 뒤에 승인자 수={}", stateCode, count);
    // Step 2: TB_AA009 문서 조회
//    String aa009Sql = """
//     SELECT * FROM TB_AA009
//       WHERE appnum = :appnum
//          OR spdate  + spnum + SPJANGCD = :appnum;
//  """;
//    List<Map<String, Object>> AA009Rows = sqlRunner.getRows(aa009Sql, params);
//
//    if (AA009Rows != null && !AA009Rows.isEmpty()) {
//      log.info("✅ TB_AA009 문서 찾음: appnum={}", appnum);
//
//      StringBuilder updateSql = new StringBuilder("""
//        UPDATE TB_AA009
//        SET appgubun = :action,
//            inputdate = CURRENT_DATE
//    """);
//
//      if (remark != null && !remark.trim().isEmpty()) {
//        updateSql.append(", remark = :remark");
//        params.addValue("remark", remark);
//      }
//
//      updateSql.append("""
//        WHERE appnum = :appnum
//           OR spdate + spnum + SPJANGCD = :appnum
//    """);
//
//      params.addValue("action", stateCode);
//
//      int aa009Affected = sqlRunner.execute(updateSql.toString(), params);
//      log.info("📝 TB_AA009 업데이트 완료: 변경된 row 수 = {}", aa009Affected);
//    } else {
//      log.warn("❌ TB_AA009 문서 찾지 못함: appnum={}", appnum);
//      return false;
//    }
// Step 3: TB_E080 업데이트 (현재 결재자만 대상)
    StringBuilder updateE080Sql = new StringBuilder("""
    UPDATE TB_E080
    SET appgubun = :action,
        remark = :remark,
""");

    if ("001".equals(stateCode)) {
      updateE080Sql.append("        appdate = NULL\n");
    } else {
      updateE080Sql.append("        appdate = TO_CHAR(CURRENT_DATE, 'YYYYMMDD')\n");
    }

    updateE080Sql.append("""
    WHERE appnum = :appnum
      AND personid = :currentAppperid
      AND papercd = :papercd
""");

    params.addValue("action", stateCode);
    params.addValue("remark", remark);
    params.addValue("currentAppperid", currentAppperid);
    params.addValue("papercd", papercd);

    int e080Affected = sqlRunner.execute(updateE080Sql.toString(), params);
    log.info("📝 TB_E080 업데이트 완료: 변경된 row 수 = {}", e080Affected);

    // Step 4: 상태코드에 따른 flag 처리
    if ("101".equals(stateCode) || "001".equals(stateCode)) {
      // 1. 현재 결재자 seq 가져오기
      String getSeqSql = """
      SELECT seq FROM TB_E080
      WHERE appnum = :appnum
        AND personid = :currentAppperid
  """;
      Object seqObj = sqlRunner.getRow(getSeqSql, params).get("seq");

      int currentSeq = 0;
      if (seqObj instanceof Number) {
        currentSeq = ((Number) seqObj).intValue();
      } else if (seqObj instanceof String) {
        currentSeq = Integer.parseInt((String) seqObj);
      }
      params.addValue("currentSeq", currentSeq);

      // 2. 다음 결재자 찾기
      String findNextSql = """
      SELECT TOP 1 seq FROM TB_E080
      WHERE appnum = :appnum
        AND seq > :currentSeq
        AND flag = """ + ("101".equals(stateCode) ? "0" : "1") + """
      ORDER BY seq ASC
  """;
      Map<String, Object> nextRow = sqlRunner.getRow(findNextSql, params);

      if (nextRow != null && nextRow.get("seq") != null) {
        Object nextSeqObj = nextRow.get("seq");
        int nextSeq = 0;
        if (nextSeqObj instanceof Number) {
          nextSeq = ((Number) nextSeqObj).intValue();
        } else if (nextSeqObj instanceof String) {
          nextSeq = Integer.parseInt((String) nextSeqObj);
        }

        String updateFlagSql = """
        UPDATE TB_E080
        SET flag = """ + ("101".equals(stateCode) ? "1" : "0") + """
        WHERE appnum = :appnum
          AND seq = :nextSeq
    """;
        MapSqlParameterSource nextParams = new MapSqlParameterSource();
        nextParams.addValue("appnum", appnum);
        nextParams.addValue("nextSeq", nextSeq);

        int affected = sqlRunner.execute(updateFlagSql, nextParams);
        log.info("🔄 다음 결재자 flag = {} → 완료 (seq = {})",
            "101".equals(stateCode) ? "1" : "0", nextSeq);
      } else {
        log.info("📭 다음 결재자 없음 → 최종 승인자 또는 초기화 대상 없음");
      }
    }

    return e080Affected > 0;
  }


  // 휴가 문서 상태 변경 (TB_PB204, TB_E080)
  public boolean updateStateForV(String appnum, String appgubun, String stateCode, String remark, Integer currentAppperid, String papercd) {
    MapSqlParameterSource params = new MapSqlParameterSource();
    params.addValue("appnum", appnum);

    // Step 1: TB_E080 결재라인 전체 조회
    String TB_E080Sql = """
    SELECT COUNT(*) AS cnt
    FROM TB_E080
    WHERE appnum = :appnum
      AND seq > (
        SELECT seq
        FROM TB_E080
        WHERE appnum = :appnum
          AND personid = :currentAppperid
      )
      AND appgubun = '101'
""";

    params.addValue("appnum", appnum);
    params.addValue("currentAppperid", currentAppperid);

    Map<String, Object> row = sqlRunner.getRow(TB_E080Sql, params);
    int count = row.get("cnt") != null ? ((Number) row.get("cnt")).intValue() : 0;

  // 상태 제한 처리
    if (count > 0 && !"101".equals(stateCode)) {
      log.warn("❌ 내 뒤에 있는 사람이 이미 승인함 → 승인 외 상태 변경 불가 (요청: {})", stateCode);
      return false;
    }

    log.info("✅ 상태 변경 가능: stateCode={}, 뒤에 승인자 수={}", stateCode, count);

    // Step 2: TB_PB204 문서 조회
    String PB204Sql = """
      SELECT * FROM TB_PB204
      WHERE appnum = :appnum
  """;
    List<Map<String, Object>> TB_PB204Rows = sqlRunner.getRows(PB204Sql, params);

    if (TB_PB204Rows != null && !TB_PB204Rows.isEmpty()) {
      log.info("✅ TB_PB204 문서 찾음: appnum={}", appnum);

      // remark 유무에 따라 동적 쿼리 구성
      StringBuilder updateSql = new StringBuilder("""
        UPDATE TB_PB204
        SET appgubun = :action,
            appdate = TO_CHAR(CURRENT_DATE, 'YYYYMMDD')
    """);

      if (remark != null && !remark.trim().isEmpty()) {
        updateSql.append(", remark = :remark");
        params.addValue("remark", remark);
      }

      updateSql.append("""
        WHERE appnum = :appnum
    """);

      params.addValue("action", stateCode);

      int affected = sqlRunner.execute(updateSql.toString(), params);
      log.info("📝 TB_PB204 업데이트 완료: 변경된 row 수 = {}", affected);
    } else {
      log.warn("❌ TB_PB204에서 문서 찾지 못함: appnum={}", appnum);
      return false;
    }

    // Step 3: TB_E080 업데이트 (현재 결재자만 대상)
    StringBuilder updateE080Sql = new StringBuilder("""
    UPDATE TB_E080
    SET appgubun = :action
""");

    if ("001".equals(stateCode)) {
      updateE080Sql.append("       , repodate = NULL\n");
    } else {
      updateE080Sql.append("       , repodate = TO_CHAR(CURRENT_DATE, 'YYYYMMDD')\n");
    }

    updateE080Sql.append("""
    WHERE appnum = :appnum
      AND personid = :currentAppperid
      AND papercd = :papercd
""");

    params.addValue("action", stateCode);
    params.addValue("remark", remark);
    params.addValue("currentAppperid", currentAppperid);
    params.addValue("papercd", String.valueOf(papercd));

    int e080Affected = sqlRunner.execute(updateE080Sql.toString(), params);
    log.info("📝 TB_E080 업데이트 완료: 변경된 row 수 = {}", e080Affected);


    // Step 4: 상태코드에 따른 flag 처리
    if ("101".equals(stateCode) || "001".equals(stateCode)) {
      // 1. 현재 결재자 seq 가져오기
      String getSeqSql = """
      SELECT seq FROM TB_E080
      WHERE appnum = :appnum
        AND personid = :currentAppperid
  """;
      Object seqObj = sqlRunner.getRow(getSeqSql, params).get("seq");

      int currentSeq = 0;
      if (seqObj instanceof Number) {
        currentSeq = ((Number) seqObj).intValue();
      } else if (seqObj instanceof String) {
        currentSeq = Integer.parseInt((String) seqObj);
      }
      params.addValue("currentSeq", currentSeq);

      int seqCursor = currentSeq;
      Map<String, Object> nextRow;

      String flagValue = "101".equals(stateCode) ? "0" : "1";

      while (true) {
        String findNextSql = """
            SELECT seq, gubun FROM TB_E080
            WHERE appnum = :appnum
              AND seq > :seqCursor
              AND flag = :flag
            ORDER BY seq ASC
            LIMIT 1
        """;

        MapSqlParameterSource findParams = new MapSqlParameterSource();
        findParams.addValue("appnum", appnum);
        findParams.addValue("seqCursor", seqCursor);
        findParams.addValue("flag", flagValue);

        nextRow = sqlRunner.getRow(findNextSql, findParams);
        if (nextRow == null) {
          log.info("✅ 더 이상 다음 결재자가 없습니다.");
          break;
        }

        String gubun = String.valueOf(nextRow.get("gubun"));
        int nextSeq = ((Number) nextRow.get("seq")).intValue();

        if ("121".equals(gubun)) {
          log.info("📎 참조자 발견 (seq = {}), 건너뜀", nextSeq);
          seqCursor = nextSeq;
          continue;
        }

        // 승인자 발견 → flag 업데이트
        String updateFlagSql = """
        UPDATE TB_E080
        SET flag = """ + ("101".equals(stateCode) ? "1" : "0") + """
        WHERE appnum = :appnum
          AND seq = :nextSeq
    """;

        MapSqlParameterSource nextParams = new MapSqlParameterSource();
        nextParams.addValue("appnum", appnum);
        nextParams.addValue("nextSeq", nextSeq);

        sqlRunner.execute(updateFlagSql, nextParams);
        log.info("🔄 다음 결재자 flag = {} → 완료 (seq = {})",
                "101".equals(stateCode) ? "1" : "0", nextSeq);
        break;
      }
    }

    return e080Affected > 0;
  }

  public boolean canCancelApproval(String appnum, Integer personid) {
    MapSqlParameterSource params = new MapSqlParameterSource();
    params.addValue("appnum", appnum);
    params.addValue("personid", personid);

    // 1. 내 seq 조회
    String seqSql = """
        SELECT seq
        FROM TB_E080
        WHERE appnum = :appnum
          AND personid = :personid
    """;
    String mySeq = sqlRunner.queryForObject(seqSql, params, (rs, rowNum) -> rs.getString(1));
    if (mySeq == null) {
      return false;
    }

    // 2. 내 seq보다 뒤에 결재자 중 이미 승인한 사람이 있는지 확인
    String checkSql = """
        SELECT COUNT(1)
        FROM TB_E080
        WHERE appnum = :appnum
          AND seq > :mySeq
          AND appgubun = '101'
    """;
    params.addValue("mySeq", mySeq);
    int approvedAfterMe = sqlRunner.queryForCount(checkSql, params);

    if (approvedAfterMe > 0) {
      log.info("❌ 뒤에 결재자가 이미 승인함 → 취소 불가");
      return false;
    }

    log.info("✅ 취소 가능: 뒤에 승인 없음");
    return true;
  }


  public boolean isAlreadyApproved(String appnum) {
    String sql = """
        SELECT COUNT(1)
        FROM TB_E080
        WHERE appnum = :appnum AND repodate IS NOT NULL AND appgubun != '001'
    """;
    MapSqlParameterSource params = new MapSqlParameterSource()
        .addValue("appnum", appnum);
//        .addValue("appperid", appperid);
    return sqlRunner.queryForCount(sql, params) > 0;
  }

  public String getAgencyName() {
    String sql = "SELECT spjangnm FROM tb_xa012";
    MapSqlParameterSource param = new MapSqlParameterSource();
    Map<String, Object> row = sqlRunner.getRow(sql, param);

    return (row != null && row.get("spjangnm") != null)
        ? row.get("spjangnm").toString()
        : "기관명 없음";
  }


}
