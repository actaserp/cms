package mes.app.PaymentList.service;

import lombok.extern.slf4j.Slf4j;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;


@Slf4j
@Service
public class PdfService {

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Autowired
  SqlRunner sqlRunner;

  public byte[] getPdfByKey(String key) {
    try {
//      log.info("🔹 PDF 데이터 조회 시작: key={}", key);

      // 📌 key 값의 앞뒤 공백 제거
      String trimmedKey = key.trim();
//      log.info("🔹 공백 제거된 key: {}", trimmedKey);

      // 📌 SQL 파라미터 설정
      MapSqlParameterSource params = new MapSqlParameterSource();
      params.addValue("file_key", trimmedKey);

      // 📌 SQL 실행 로그 추가
      String sql = """
            SELECT CAST(pdf_data AS VARBINARY(MAX)) AS pdf_data 
            FROM TB_AA010PDF 
            WHERE LTRIM(RTRIM(spdate)) = :file_key
        """;
//      log.info("🔹 실행할 SQL: {}", sql);
//      log.info("🔹 SQL 파라미터: file_key={}", trimmedKey);

      // 📌 SQL 실행
      Map<String, Object> result = sqlRunner.getRow(sql, params);

      // 📌 결과 확인 후 변환
      if (result != null && result.containsKey("pdf_data")) {
        byte[] pdfData = (byte[]) result.get("pdf_data");
//        log.info("✅ PDF 데이터 조회 성공: key={}", key);
        return pdfData;
      } else {
//        log.warn("❌ PDF 데이터 없음: key={}", key);
        return null;
      }
    } catch (Exception e) {
//      log.error("🚨 PDF 데이터 조회 중 오류 발생: key={}, error={}", key, e.getMessage(), e);
      return null;
    }
  }

  // 📌 spdate를 기반으로 custcd 조회
  public String getCustcdBySpdate(String fileKey) {
    try {
//      log.info("🔹 회사 코드(custcd) 조회 시작: fileKey={}", fileKey);
      String sql = """
                SELECT custcd 
                FROM tb_xa012 
                WHERE spjangcd = (SELECT spjangcd FROM TB_AA010PDF WHERE spdate = ?)
            """;
      String custcd = jdbcTemplate.queryForObject(sql, new Object[]{fileKey}, String.class);
//      log.info("✅ 회사 코드 조회 성공: fileKey={}, custcd={}", fileKey, custcd);
      return custcd;
    } catch (EmptyResultDataAccessException e) {
//      log.warn("❌ 회사 코드 없음: fileKey={}", fileKey);
      return null;
    } catch (Exception e) {
      log.error("🚨 회사 코드 조회 중 오류 발생: fileKey={}, error={}", fileKey, e.getMessage(), e);
      return null;
    }
  }

  //첨부파일
  public byte[] getPdfByKeyForA(String key) {
    try {
//      log.info("🔹 첨부파일 데이터 조회 시작: key={}", key);

      // 📌 key 값의 앞뒤 공백 제거
      String trimmedKey = key.trim();
//      log.info("🔹 공백 제거된 key: {}", trimmedKey);

      // 📌 SQL 파라미터 설정
      MapSqlParameterSource params = new MapSqlParameterSource();
      params.addValue("file_key", trimmedKey);

      // 📌 SQL 실행 로그 추가
      String sql = """
            SELECT
            CAST(pdf_data AS VARBINARY(MAX)) AS pdf_data 
            FROM TB_AA010ATCH 
            WHERE LTRIM(RTRIM(spdate)) = :file_key
        """;
//      log.info("🔹 실행할 SQL: {}", sql);
//      log.info("🔹 SQL 파라미터: file_key={}", trimmedKey);

      // 📌 SQL 실행
      Map<String, Object> result = sqlRunner.getRow(sql, params);

      // 📌 결과 확인 후 변환
      if (result != null && result.containsKey("pdf_data")) {
        byte[] pdfData = (byte[]) result.get("pdf_data");
       // log.info("✅ 첨부파일 데이터 조회 성공: key={}", key);
        return pdfData;
      } else {
        log.warn("❌ 첨부파일 데이터 없음: key={}", key);
        return null;
      }
    } catch (Exception e) {
      log.error("🚨 첨부파일 데이터 조회 중 오류 발생: key={}, error={}", key, e.getMessage(), e);
      return null;
    }
  }

  // 📌 A로 시작하는 key에 대해 다른 테이블에서 고객 코드 조회
  public String getCustcdBySpdateForA(String fileKey) {
    try {
//      log.info("🔹 A 전용 고객 코드 조회 시작: fileKey={}", fileKey);

      // 📌 SQL 쿼리 (A 전용 테이블에서 조회)
      String sql = """
            SELECT custcd 
            FROM tb_xa012 
            WHERE spjangcd = (SELECT spjangcd FROM TB_AA010ATCH WHERE spdate = ?)
        """;

      // 📌 JDBC를 이용해 조회
      String custcd = jdbcTemplate.queryForObject(sql, new Object[]{fileKey}, String.class);
//      log.info("✅ A 전용 고객 코드 조회 성공: fileKey={}, custcd={}", fileKey, custcd);
      return custcd;

    } catch (EmptyResultDataAccessException e) {
      log.warn("❌ A 전용 고객 코드 없음: fileKey={}", fileKey);
      return null;
    } catch (Exception e) {
      log.error("🚨 A 전용 고객 코드 조회 중 오류 발생: fileKey={}, error={}", fileKey, e.getMessage(), e);
      return null;
    }
  }

  @Transactional
  public boolean updateFilePath(String key, String filePath) {
    try {
      int updatedRows;
      if (key.startsWith("A")) {
        // "A"로 시작하는 경우 A 테이블 업데이트
//        log.info("🔹 A용 테이블 업데이트: key={}, filePath={}", key, filePath);

        String sql = "UPDATE TB_AA010ATCH SET filepath = ? WHERE spdate = ?";
        updatedRows = jdbcTemplate.update(sql, filePath, key);

      } else {
        // 일반 테이블 업데이트
//        log.info("🔹 기존 테이블 업데이트: key={}, filePath={}", key, filePath);

        String sql = "UPDATE TB_AA010PDF SET filepath = ? WHERE spdate = ?";
        updatedRows = jdbcTemplate.update(sql, filePath, key);
      }

      return updatedRows > 0;  // 하나라도 업데이트되었으면 true 반환
    } catch (Exception e) {
      log.error("🚨 파일 경로 업데이트 중 오류 발생: key={}, filePath={}, error={}", key, filePath, e.getMessage(), e);
      return false;
    }
  }

  public String getFilenameByKeyForA(String key) {
    try {
//      log.info("🔹 첨부파일 데이터 조회 시작: key={}", key);

      // 📌 key 값의 앞뒤 공백 제거
      String trimmedKey = key.trim();
//      log.info("🔹 공백 제거된 key: {}", trimmedKey);

      // 📌 SQL 파라미터 설정
      MapSqlParameterSource params = new MapSqlParameterSource();
      params.addValue("file_key", trimmedKey);

      // 📌 SQL 실행 로그 추가
      String sql = """
            SELECT filename
            FROM TB_AA010ATCH 
            WHERE LTRIM(RTRIM(spdate)) = :file_key
        """;
//      log.info("🔹 실행할 SQL: {}", sql);
//      log.info("🔹 SQL 파라미터: file_key={}", trimmedKey);

      // 📌 SQL 실행
      Map<String, Object> result = sqlRunner.getRow(sql, params);

      // 📌 결과 확인 후 변환
      if (result != null && result.containsKey("filename")) {
        Object value = result.get("filename");
        String filename = (value != null) ? value.toString() : null;
        return filename;
      } else {
//        log.warn("❌ 첨부파일 데이터 없음: key={}", key);
        return null;
      }
    } catch (Exception e) {
      log.error("🚨 첨부파일 데이터 조회 중 오류 발생: key={}, error={}", key, e.getMessage(), e);
      return null;
    }
  }

  public String getFilenameByKey(String key) {
    try {
//      log.info("🔹 PDF 데이터 조회 시작: key={}", key);

      // 📌 key 값의 앞뒤 공백 제거
      String trimmedKey = key.trim();
//      log.info("🔹 공백 제거된 key: {}", trimmedKey);

      // 📌 SQL 파라미터 설정
      MapSqlParameterSource params = new MapSqlParameterSource();
      params.addValue("file_key", trimmedKey);

      // 📌 SQL 실행 로그 추가
      String sql = """
            SELECT filename
            FROM TB_AA010PDF 
            WHERE LTRIM(RTRIM(spdate)) = :file_key
        """;
//      log.info("🔹 실행할 SQL: {}", sql);
//      log.info("🔹 SQL 파라미터: file_key={}", trimmedKey);

      // 📌 SQL 실행
      Map<String, Object> result = sqlRunner.getRow(sql, params);

      // 📌 결과 확인 후 변환
      if (result != null && result.containsKey("filename")) {
        Object value = result.get("filename");
        String filename = (value != null) ? value.toString() : null;
        return filename;
      } else {
        log.warn("❌ PDF 데이터 없음: key={}", key);
        return null;
      }
    } catch (Exception e) {
      log.error("🚨 PDF 데이터 조회 중 오류 발생: key={}, error={}", key, e.getMessage(), e);
      return null;
    }
  }
}