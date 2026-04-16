package mes.app.PaymentList;

import lombok.extern.slf4j.Slf4j;
import mes.app.PaymentList.service.ApprovalListService;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.UserCodeRepository;
//import mes.domain.repository.approval.TB_AA010ATCHRepository;
//import mes.domain.repository.approval.tb_aa010Repository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Slf4j
@RestController
@RequestMapping("/api/PaymentList")
public class ApprovalListController { //결재목록

  @Autowired
  private UserCodeRepository userCodeRepository;

  @Autowired
  private ApprovalListService approvalListService;

//  @Autowired
//  tb_aa010Repository tbAa010PdfRepository;
//
//  @Autowired
//  TB_AA010ATCHRepository tbAa010AtchRepository;

  @GetMapping("/read")
  public AjaxResult getPaymentList(@RequestParam(value = "startDate") String startDate,
                                   @RequestParam(value = "endDate") String endDate,
                                   @RequestParam(value = "search_spjangcd", required = false) String spjangcd,
                                   @RequestParam(value = "SearchPayment", required = false) String SearchPayment,
                                   @RequestParam(value = "searchUserNm", required = false) String searchUserNm,
                                   Authentication auth) {
    AjaxResult result = new AjaxResult();
    log.info("주문 확인 read 들어온 데이터:startDate{}, endDate{}, spjangcd {}, SearchPayment {} ,searchUserNm {} ", startDate, endDate, spjangcd, SearchPayment,searchUserNm);

    try {

      User user = (User) auth.getPrincipal();
//      String agencycd = user.getAgencycd().replaceFirst("^p", "");
      Integer personid = user.getPersonid();
      LocalDate dateStart = LocalDate.parse(startDate); // 기본 ISO-8601 형식 사용
      String formattedStartDate = dateStart.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
      LocalDate dateEnd = LocalDate.parse(endDate); // 기본 ISO-8601 형식 사용
      String formattedEndDate = dateEnd.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
      // 데이터 조회
      List<Map<String, Object>> getPaymentList = approvalListService.getPaymentList(spjangcd, formattedStartDate, formattedEndDate, SearchPayment,searchUserNm, personid);
      log.info("📦 [조회결과] 결재 목록 건수: {}", getPaymentList.size());
      for (Map<String, Object> item : getPaymentList) {
        //날짜 포맷 변환 (repodate)
        formatDateField(item, "repodate");
        //날짜 포맷 변환 (appdate)
        formatDateField(item, "indate");

        String appnum = (String) item.get("appnum");
        List<Map<String, Object>> fileList = new ArrayList<>();

//        if (appnum != null) {
//          if (appnum.startsWith("AS")) {
//            if (fileExistsInAtchTable(appnum)) {
//              Map<String, Object> atch = new HashMap<>(createFileMapFromAtch(appnum, "첨부파일"));
//              atch.put("fileType", "첨부");
//              fileList.add(atch);
//              log.debug("📎 AS 첨부파일 추가: {}", atch);
//            }
//            if (fileExistsInPdfTable(appnum)) {
//              Map<String, Object> pdf = new HashMap<>(createFileMapFromPdf(appnum, "지출결의서"));
//              pdf.put("fileType", "전표");
//              fileList.add(pdf);
//              log.debug("📄 AS 전표파일 추가: {}", pdf);
//            }
//
//          } else if (appnum.startsWith("A")) {
//            if (fileExistsInAtchTable(appnum)) {
//              Map<String, Object> atch = new HashMap<>(createFileMapFromAtch(appnum, "첨부파일"));
//              atch.put("fileType", "첨부");
//              fileList.add(atch);
//              log.debug("📎 A 첨부파일 추가: {}", atch);
//            }
//
//          } else if (appnum.startsWith("S")) {
//            if (fileExistsInPdfTable(appnum)) {
//              Map<String, Object> pdf = new HashMap<>(createFileMapFromPdf(appnum, "지출결의서"));
//              pdf.put("fileType", "전표");
//              fileList.add(pdf);
//              log.debug("📄 S 전표파일 추가: {}", pdf);
//            }
//
//          } else {
//            if (fileExistsInPdfTable(appnum)) {
//              Map<String, Object> pdf = new HashMap<>(createFileMapFromPdf(appnum, "전표파일"));
//              pdf.put("fileType", "전표");
//              fileList.add(pdf);
//              log.debug("📄 기타 전표파일 추가: {}", pdf);
//            }
//          }
//        }

        item.put("fileList", fileList);                  // ✅ 항상 넣고
        item.put("isdownload", !fileList.isEmpty());     // ✅ 상태 표시

      }

      // 데이터가 있을 경우 성공 메시지
      result.success = true;
      result.message = "데이터 조회 성공";
      result.data = getPaymentList;

    } catch (Exception e) {
      // 예외 처리
      log.error("❌ [에러] 결재 목록 조회 중 예외 발생", e);
      result.success = false;
      result.message = "데이터 조회 중 오류 발생: " + e.getMessage();
    }

    return result;
  }

  @GetMapping("/read1")
  public AjaxResult getPaymentList1(@RequestParam(value = "startDate") String startDate,
                                   @RequestParam(value = "endDate") String endDate,
                                   @RequestParam(value = "search_spjangcd", required = false) String spjangcd,
                                   Authentication auth) {
    AjaxResult result = new AjaxResult();
    log.info("결재목록_문서현황 read 들어온 데이터:startDate{}, endDate{}, spjangcd {} ", startDate, endDate, spjangcd);

    try {

      User user = (User) auth.getPrincipal();
//      String agencycd = user.getAgencycd().replaceFirst("^p", "");
      LocalDate dateStart = LocalDate.parse(startDate); // 기본 ISO-8601 형식 사용
      String formattedStartDate = dateStart.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
      LocalDate dateEnd = LocalDate.parse(endDate); // 기본 ISO-8601 형식 사용
      String formattedEndDate = dateEnd.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
      Integer personid = user.getPersonid();
      String userName = user.getFirst_name();
      // 데이터 조회
      List<Map<String, Object>> getPaymentList = approvalListService.getPaymentList1(spjangcd, formattedStartDate, formattedEndDate, personid);


      // 데이터가 있을 경우 성공 메시지
      result.success = true;
      result.message = "데이터 조회 성공";
      result.data = Map.of(
          "userName", userName,  // 사용자 이름
          "paymentList", getPaymentList // 결재 목록 리스트
      );

    } catch (Exception e) {
      // 예외 처리
      result.success = false;
      result.message = "데이터 조회 중 오류 발생: " + e.getMessage();
    }

    return result;
  }

  @GetMapping("/read2")
  public AjaxResult getPaymentList2(@RequestParam(value = "search_spjangcd", required = false) String spjangcd,
                                    @RequestParam(value = "appnum", required = false) String appnum) {
    AjaxResult result = new AjaxResult();
    log.info("더블클릭(결재목록) 들어온 데이터:spjangcd {}, appnum: {} ", spjangcd, appnum);

    try {

      List<Map<String, Object>> getPaymentList2 = approvalListService.getPaymentList2(spjangcd,appnum);

      for (Map<String, Object> item : getPaymentList2) {
        //날짜 포맷
        formatDateField(item, "repodate");
      }

      result.success = true;
      result.message = "데이터 조회 성공";
      result.data = getPaymentList2;
    } catch (Exception e) {
      // 예외 처리
      result.success = false;
      result.message = "데이터 조회 중 오류 발생: " + e.getMessage();
    }

    return result;
  }

  // 날짜 포맷
  private void formatDateField(Map<String, Object> item, String fieldName) {
    Object dateValue = item.get(fieldName);
    if (dateValue instanceof String) {
      String dateStr = (String) dateValue;
      try {
        if (dateStr.length() == 8) { // "yyyyMMdd" 형식인지 확인
          String formattedDate = dateStr.substring(0, 4) + "-" + dateStr.substring(4, 6) + "-" + dateStr.substring(6, 8);
          item.put(fieldName, formattedDate);
        } else {
          item.put(fieldName, "잘못된 날짜 형식");
        }
      } catch (Exception ex) {
        log.error("{} 변환 중 오류 발생: {}", fieldName, ex.getMessage());
        item.put(fieldName, "잘못된 날짜 형식");
      }
    }
  }

//  @GetMapping("/payType")
//  public AjaxResult ordFlagType(
//      @RequestParam(value = "parentCode", required = false) String parentCode) {
//    AjaxResult result = new AjaxResult();
//
//    try {
//      // parentCode를 기준으로 하위 그룹 필터링
//      List<UserCode> data = (parentCode != null)
//          ? userCodeRepository.findByParentId(userCodeRepository.findByCode(parentCode).stream().findFirst().get().getId())
//          : userCodeRepository.findAll();
//
//      // 성공 시 데이터와 메시지 설정
//      result.success = true;
//      result.message = "데이터 조회 성공";
//      result.data = data;
//
//    } catch (Exception e) {
//      // 예외 발생 시 처리
//      result.success = false;
//      result.message = "데이터 조회 중 오류 발생: " + e.getMessage();
//    }
//
//    return result;
//  }

  @GetMapping("/bindSpjangcd")
  public AjaxResult bindSpjangcd(Authentication auth) {
    // 관리자 사용가능 페이지 사업장 코드 선택 로직
    User user = (User) auth.getPrincipal();
    String username = user.getUsername();
    String spjangcd = approvalListService.getSpjangcd(username, "");
    // 사업장 코드 선택 로직 종료 반환 spjangcd 활용
    AjaxResult result = new AjaxResult();
    result.data = spjangcd;
    return result;
  }

//  private boolean fileExistsInPdfTable(String appnum) {
//    return tbAa010PdfRepository.existsBySpdateAndFilenameIsNotNull(appnum);
//  }
//
//  private boolean fileExistsInAtchTable(String appnum) {
//    return tbAa010AtchRepository.existsBySpdateAndFilenameIsNotNull(appnum);
//  }
//
//  private Map<String, Object> createFileMapFromPdf(String appnum, String label) {
//    var entity = tbAa010PdfRepository.findBySpdate(appnum);
//    return Map.of(
//        "filepath", entity.getFilepath(),
//        "filesvnm", entity.getFilename(),
//        "fileornm", label
//    );
//  }
//
//  private Map<String, Object> createFileMapFromAtch(String appnum, String label) {
//    var entity = tbAa010AtchRepository.findBySpdate(appnum);
//    return Map.of(
//        "filepath", entity.getFilepath(),
//        "filesvnm", entity.getFilename(),
//        "fileornm", label
//    );
//  }

  @PostMapping("/downloader")
  public ResponseEntity<?> downloadFile(@RequestBody List<Map<String, Object>> downloadList) throws IOException {

    log.info("다운로드 들어옴");

    // 파일 목록과 파일 이름을 담을 리스트 초기화
    List<File> filesToDownload = new ArrayList<>();
    List<String> fileNames = new ArrayList<>();

    // ZIP 파일 이름을 설정할 변수 초기화
    String tketcrdtm = null;
    String tketnm = null;

    // 파일을 메모리에 쓰기
    for (Map<String, Object> fileInfo : downloadList) {
      String filePath = (String) fileInfo.get("filepath");    // 파일 경로
      String fileName = (String) fileInfo.get("filesvnm");    // 파일 이름(uuid)
      String originFileName = (String) fileInfo.get("fileornm");  //파일 원본이름(origin Name)

      File file = new File(filePath); // filePath = 전체 경로

      // 파일이 실제로 존재하는지 확인
      if (file.exists()) {
        filesToDownload.add(file);
        fileNames.add(fileName); // 다운로드 받을 파일 이름을 originFileName으로 설정
      }
    }

    // 파일이 없는 경우
    if (filesToDownload.isEmpty()) {
      return ResponseEntity.notFound().build();
    }

    // 파일이 하나인 경우 그 파일을 바로 다운로드
    if (filesToDownload.size() == 1) {
      File file = filesToDownload.get(0);
      String originFileName = fileNames.get(0); // originFileName 가져오기

      HttpHeaders headers = new HttpHeaders();
      String encodedFileName = URLEncoder.encode(originFileName, StandardCharsets.UTF_8.toString()).replaceAll("\\+", "%20");
      headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=*''" + encodedFileName);
      headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
      headers.setContentLength(file.length());

      ByteArrayResource resource = new ByteArrayResource(Files.readAllBytes(file.toPath()));

      return ResponseEntity.ok()
          .headers(headers)
          .body(resource);
    }

    String zipFileName = (tketcrdtm != null && tketnm != null) ? tketcrdtm + "_" + tketnm + ".zip" : "download.zip";

    // 파일이 두 개 이상인 경우 ZIP 파일로 묶어서 다운로드
    ByteArrayOutputStream zipBaos = new ByteArrayOutputStream();
    try (ZipOutputStream zipOut = new ZipOutputStream(zipBaos)) {

      Set<String> addedFileNames = new HashSet<>(); // 이미 추가된 파일 이름을 저장할 Set
      int fileCount = 1;

      for (int i = 0; i < filesToDownload.size(); i++) {
        File file = filesToDownload.get(i);
        String originFileName = fileNames.get(i); // originFileName 가져오기

        // 파일 이름이 중복될 경우 숫자를 붙여 고유한 이름으로 만듦
        String uniqueFileName = originFileName;
        while (addedFileNames.contains(uniqueFileName)) {
          uniqueFileName = originFileName.replace(".", "_" + fileCount++ + ".");
        }

        // 고유한 파일 이름을 Set에 추가
        addedFileNames.add(uniqueFileName);

        try (FileInputStream fis = new FileInputStream(file)) {
          ZipEntry zipEntry = new ZipEntry(originFileName);
          zipOut.putNextEntry(zipEntry);

          byte[] buffer = new byte[1024];
          int len;
          while ((len = fis.read(buffer)) > 0) {
            zipOut.write(buffer, 0, len);
          }

          zipOut.closeEntry();
        } catch (IOException e) {
          e.printStackTrace();
          return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
      }

      zipOut.finish();
    } catch (IOException e) {
      e.printStackTrace();
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    }

    ByteArrayResource zipResource = new ByteArrayResource(zipBaos.toByteArray());

    HttpHeaders headers = new HttpHeaders();
    String encodedZipFileName = URLEncoder.encode(zipFileName, StandardCharsets.UTF_8.toString()).replaceAll("\\+", "%20");
    headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=*''" + encodedZipFileName);
    headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
    headers.setContentLength(zipResource.contentLength());

    return ResponseEntity.ok()
        .headers(headers)
        .body(zipResource);
  }

}
