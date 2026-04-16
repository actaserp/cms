package mes.app.PaymentList;

import lombok.extern.slf4j.Slf4j;
import mes.app.PaymentList.service.PdfService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

@Slf4j
@RestController
@RequestMapping("/appkey")
public class FileMkController {

  @Autowired
  private PdfService pdfService;

  @GetMapping
  public ResponseEntity<String> generatePdf(@RequestParam String key) {
    try {
      log.info("🔹 요청 수신: key={}", key);

      byte[] pdfData = null;
      String custcd = null;
      String filename = null;
      String originalKey = key;

      // ▶ A로 시작하는 경우 처리
      if (key.startsWith("A")) {
        log.info("🔹 A로 시작하는 key 감지, A 테이블 우선 조회: key={}", key);
        pdfData = pdfService.getPdfByKeyForA(key);
        custcd = pdfService.getCustcdBySpdateForA(key);
        filename = pdfService.getFilenameByKeyForA(key);

        if (pdfData != null) {
          String filePath = processPdfFile(key, pdfData, custcd, filename);

          // 🔥 여기서 A용 테이블에 경로 업데이트 수행!
          boolean isUpdated = pdfService.updateFilePath(key, filePath);
          if (!isUpdated) {
            log.warn("⚠️ A 테이블 경로 업데이트 실패: {}", filePath);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("파일 저장 완료, A 테이블 경로 업데이트 실패");
          }

          //A 테이블에서 성공했으면 더 이상 일반 테이블 조회 안 해도 되니 return 해버려도 OK
          return ResponseEntity.ok("PDF 파일 생성 및 A 테이블 경로 업데이트 완료: " + filePath);
        } else {
          log.warn("❌ A 테이블에서 PDF 데이터 없음, 일반 테이블 처리로 진행: key={}", key);
        }

        key = key.substring(1); // A 제거 후 일반 테이블 재시도
      }

      // ▶ A 제거 후, J로 시작하면 다시 제거
      if (key.startsWith("J")) {
        log.info("🔹 J로 시작하는 key 감지, 일반 key로 재조회 위해 J 제거: key={}", key);
        key = key.substring(1); // J 제거
      }

      // ▶ 일반 테이블 조회
      log.info("🔄 일반 테이블 조회 시작: key={}", key);
      pdfData = pdfService.getPdfByKey(key);
      custcd = pdfService.getCustcdBySpdate(key);
      filename = pdfService.getFilenameByKey(key);

      // 데이터 체크
      if (pdfData == null) {
        log.warn("❌ PDF 데이터 없음: key={}", key);
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body("PDF 데이터를 찾을 수 없습니다.");
      }
      if (custcd == null || custcd.isEmpty()) {
        log.warn("❌ 고객 코드 없음: key={}", key);
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body("고객 코드를 찾을 수 없습니다.");
      }

      // 공통 처리
      String filePath = processPdfFile(key, pdfData, custcd, filename);

      // DB 업데이트
      boolean isUpdated = pdfService.updateFilePath(key, filePath);
      if (!isUpdated) {
        log.warn("⚠️ 경로 업데이트 실패: {}", filePath);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("파일 저장 완료, 경로 업데이트 실패");
      }

      return ResponseEntity.ok("PDF 파일 생성 및 경로 업데이트 완료: " + filePath);

    } catch (Exception e) {
      log.error("🚨 오류 발생: key={}, error={}", key, e.getMessage(), e);
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("오류 발생: " + e.getMessage());
    }
  }

  // 🔁 공통 파일 처리 메서드 추출
  private String processPdfFile(String key, byte[] pdfData, String custcd, String filename) throws IOException {
    if (filename == null || filename.trim().isEmpty()) {
      filename = key + ".pdf";
      log.warn("⚠️ 파일명 없음 -> 기본 파일명 사용: {}", filename);
    } else if (!filename.toLowerCase().endsWith(".pdf")) {
      filename += ".pdf";
      log.info("📎 확장자 추가: {}", filename);
    }

    String directoryPath = "C:/temp/APP/" + custcd + "/";
    String filePath = directoryPath + filename;
    log.info("📂 저장 경로: {}", filePath);

    File directory = new File(directoryPath);
    if (!directory.exists()) {
      directory.mkdirs();
      log.info("📁 디렉토리 생성 완료: {}", directoryPath);
    }

    File file = new File(filePath);
    if (file.exists()) {
      file.delete();
      log.info("🗑 기존 파일 삭제 완료: {}", filePath);
    }

    Files.write(Paths.get(filePath), pdfData);
    log.info("✅ PDF 저장 완료: {}", filePath);

    return filePath;
  }

}
