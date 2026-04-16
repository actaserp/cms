package mes.app.request;

import lombok.extern.slf4j.Slf4j;
import mes.app.request.service.RequestCurrentService;
import mes.domain.entity.TbAs010;
import mes.domain.entity.TbAs020;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.TbAs010Repository;
import mes.domain.repository.TbAs011Repository;
import mes.domain.repository.TbAs020Repository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/request_current")
public class RequestCurrentController {
    @Autowired
    RequestCurrentService requestCurrentService;

    @Autowired
    private TbAs011Repository tbAs011Repository;

    @Autowired
    private TbAs010Repository tbAs010Repository;

    @Autowired
    private TbAs020Repository tbAs020Repository;

    // 요청사항 조회
    @GetMapping("/search")
    public AjaxResult searchDatas(
            HttpServletRequest request,
            @RequestParam(value="searchfrdate") String searchfrdate,
            @RequestParam(value="searchtodate") String searchtodate,
            @RequestParam(value="searchCompCd", required=false) String searchCompCd,
            @RequestParam(value="reqType", required=false) String reqType,
            @RequestParam(value="spjangcd", required=false) String spjangcd,
            @RequestParam(value="aspernm", required=false) String aspernm,
            @RequestParam(value="searchCompnm", required=false) String searchCompnm,
            @RequestParam(value="recyn", required=false) String recyn,
            Authentication auth) {
        AjaxResult result = new AjaxResult();

        User user = (User) auth.getPrincipal();
        Integer perId = user.getPersonid();

        List<Map<String, Object>> searchDatas = requestCurrentService.searchDatas(
                searchfrdate
                , searchtodate
                , searchCompCd
                , searchCompnm // 업체명
                , reqType
                , spjangcd
                , perId
                , recyn
                , aspernm // 본사담당 이름
        );

        result.data = searchDatas;

        return result;
    }

    // 상세정보 조회
    @GetMapping("/detail")
    public AjaxResult getRequestDetail(
            @RequestParam("asid") Integer asid,
            HttpServletRequest request) {
        AjaxResult result = new AjaxResult();
        
        Map<String, Object> item = requestCurrentService.getDetail(asid);
        result.data = item;
        
        return result;
    }

    // 처리내용 저장
    @PostMapping("/save")
    @Transactional
    public AjaxResult saveProcess(@RequestBody Map<String, Object> payload, Authentication auth) {
        User user = (User) auth.getPrincipal();
        AjaxResult result = new AjaxResult();

        try {
            result = requestCurrentService.saveProcess(payload, user);

            // check020(업무일지)이 1일 경우 tb_as020에도 insert
            if (payload.get("check020") != null && "1".equals(payload.get("check020").toString())) {

                // ✅ asid 추출
                Integer asid = payload.get("asid") != null && !payload.get("asid").toString().isEmpty()
                        ? Integer.parseInt(payload.get("asid").toString())
                        : null;

                if (asid == null) {
                    throw new RuntimeException("ASID가 존재하지 않아 tb_as020을 생성할 수 없습니다.");
                }

                // ✅ tb_as010 데이터 조회
                TbAs010 request = tbAs010Repository.findById(asid)
                        .orElseThrow(() -> new RuntimeException("요청사항을 찾을 수 없습니다. (asid=" + asid + ")"));

                // ✅ TbAs020 신규 엔티티 생성
                TbAs020 entity = new TbAs020();

                // 날짜 관련 처리
                String rptdate = payload.get("fixdate").toString().replaceAll("-", "");

                entity.setRptdate(rptdate);                  // 등록일자
                entity.setFrdate(rptdate);                  // 처리시작일자
                entity.setTodate(rptdate);                  // 처리종료일자
                entity.setRptweek(getWeekFromDate(rptdate)); // 작성주차 자동 계산

                // 기본값 설정
                entity.setCltnm(request.getUsernm());          // 업체명
                entity.setFixflag("1");                       // 업무구분 (기본 1)
                entity.setActflag("3");                       // 근무구분 (기본 1)
                entity.setAsmenu(request.getAsmenu());       // 화면명
                entity.setAsdv(request.getAsdv());            // 요청구분
                entity.setRecyn((String) payload.get("recyn"));                         // 진행구분 기본 완료 상태
                entity.setRptremark((String) payload.get("remark"));    // 업무내용 기본 문구
                entity.setRemark("유지보수 요청처리");   // 특이사항
                entity.setFixperid(user.getUsername());
                entity.setFixpernm(user.getFirst_name());
                entity.setInputdate(new Timestamp(System.currentTimeMillis()));

                tbAs020Repository.save(entity);
                tbAs020Repository.flush();
                System.out.println(">>> 저장 완료 후 ID: " + entity.getRptid());
            }

        } catch (Exception e) {
            e.printStackTrace();
            result.success = false;
            result.message = e.getMessage();
        }

        return result;
    }

    // 접수 처리 전용 메서드
    @PostMapping("/receive")
    @Transactional
    public AjaxResult receiveRequest(@RequestBody Map<String, Object> payload, Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();
        Integer perId = user.getPersonid();

        Integer asid = Integer.parseInt(payload.get("asid").toString());

        try {
            // 요청사항 존재 확인
            TbAs010 request = tbAs010Repository.findById(asid)
                    .orElseThrow(() -> new RuntimeException("요청사항을 찾을 수 없습니다."));

            // 현재 사용자 정보 (접수자)
            String recperid = String.valueOf(user.getPersonid());
            String recpernm = user.getFirst_name();

            // 진행구분 상태값 변경 (필요 시)
//            String newRecyn = "20";  // 예: '10=요청', '20=접수', '30=처리중' 등 코드테이블 값 사용

            // 값 세팅
            request.setRecyn("0");
            if (request.getAspernm() == null || request.getAspernm().isEmpty()) {
                request.setAsperid(recperid);
                request.setAspernm(recpernm);
            }
            request.setRecperid(recperid);
            request.setRecpernm(recpernm);
            request.setRecdate(new Timestamp(System.currentTimeMillis()));
//            request.setRecyn(newRecyn);

            // 저장
            tbAs010Repository.save(request);

            result.success = true;
            result.message = "접수가 완료되었습니다.";
            result.data = asid;

        } catch (Exception e) {
            e.printStackTrace();
            result.success = false;
            result.message = "접수 처리 중 오류: " + e.getMessage();
        }

        return result;
    }

    // 완료첨부파일 다운로드 메서드
    @GetMapping("/downFile")
    public ResponseEntity<Resource> downFile(@RequestParam("fileName") String fileName) {
        try {
            // ✅ 실제 파일 경로 (업로드 경로와 동일)
            File file = new File("C:/temp/as_request/files/" + fileName);

            if (!file.exists()) {
                log.warn("❌ 파일 없음: {}", file.getAbsolutePath());
                return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
            }

            // ✅ 리소스 래핑
            Resource resource = new FileSystemResource(file);

            // ✅ 확장자 추출 (원본 파일의 확장자 유지)
            String ext = "";
            int dotIndex = file.getName().lastIndexOf(".");
            if (dotIndex > 0) {
                ext = file.getName().substring(dotIndex); // 예: .png, .pdf 등
            }

            // ✅ 다운로드 시 표시될 파일명 (고정)
            String downloadName = "유지보수_완료_첨부파일" + ext;

            // ✅ 파일명 인코딩 (한글 깨짐 방지)
            String encodedName = URLEncoder.encode(downloadName, StandardCharsets.UTF_8)
                    .replaceAll("\\+", "%20");

            // ✅ Content-Type 자동 탐지
            String contentType = Files.probeContentType(file.toPath());
            if (contentType == null) contentType = "application/octet-stream";

            // ✅ 다운로드 응답
            return ResponseEntity.ok()
                    .contentType(MediaType.parseMediaType(contentType))
                    .header(HttpHeaders.CONTENT_DISPOSITION,
                            "attachment; filename*=UTF-8''" + encodedName)
                    .header(HttpHeaders.CONTENT_LENGTH, String.valueOf(file.length()))
                    .body(resource);

        } catch (Exception e) {
            log.error("❌ 파일 다운로드 실패", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    // 작성주차 자동 계산 메서드
    /**
     * ✅ 한국식 주차 계산 (프론트 JS와 동일)
     * - 주 시작: 월요일
     * - 1월 첫 월요일 전까지는 전년도 마지막 주로 간주
     * - 예: 2026-01-05 → "2026년 1주차"
     */
    private String getWeekFromDate(String dateStr) {
        try {
            // 1️⃣ 입력 포맷 정규화
            String normalized = dateStr.replaceAll("-", "");
            java.time.LocalDate date = java.time.LocalDate.parse(
                    normalized, java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));

            // 2️⃣ 이번 주의 월요일 계산
            java.time.DayOfWeek dow = date.getDayOfWeek();
            java.time.LocalDate monday = date.minusDays((dow.getValue() + 6) % 7);

            // 3️⃣ 주차 계산용 기준 연도 결정
            int weekYear = monday.getYear();
            if (monday.getMonthValue() == 1 && monday.getDayOfMonth() < 4) {
                weekYear -= 1; // 1월 첫 월요일 전이면 전년도 마지막 주
            }

            // 4️⃣ 해당 연도의 첫 월요일 구하기
            java.time.LocalDate firstMonday = java.time.LocalDate.of(weekYear, 1, 1);
            while (firstMonday.getDayOfWeek() != java.time.DayOfWeek.MONDAY) {
                firstMonday = firstMonday.plusDays(1);
            }

            // 5️⃣ 주차 번호 계산 (첫 월요일부터 몇 주차인지)
            long weekNo = java.time.temporal.ChronoUnit.WEEKS.between(firstMonday, monday) + 1;

            // 🔥 경계 보정: 주차 번호가 0 이하일 경우 (1월 초)
            if (weekNo <= 0) {
                weekYear -= 1;
                java.time.LocalDate lastYearLastMonday = java.time.LocalDate.of(weekYear, 12, 31);
                while (lastYearLastMonday.getDayOfWeek() != java.time.DayOfWeek.MONDAY) {
                    lastYearLastMonday = lastYearLastMonday.minusDays(1);
                }
                weekNo = java.time.temporal.ChronoUnit.WEEKS.between(
                        lastYearLastMonday, monday) + 1;
            }

            // 6️⃣ 형식화하여 반환
            return String.format("%d년 %d주차", weekYear, weekNo);

        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

}
