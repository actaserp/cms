package mes.app.dashboard;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import mes.app.dashboard.service.DashSummaryService;
import mes.domain.entity.TbAs010;
import mes.domain.entity.User;
import mes.domain.entity.UserCode;
import mes.domain.entity.mobile.TB_PB204;
import mes.domain.model.AjaxResult;
import mes.domain.repository.TbAs010Repository;
import mes.domain.repository.UserCodeRepository;
import mes.domain.repository.mobile.TB_PB204Repository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.*;
import java.util.stream.Collectors;


@Slf4j
@RestController
@RequestMapping("/api/dash_summary")
public class DashSummaryController {

    @Autowired
    DashSummaryService dashSummaryService;
    @Autowired
    private UserCodeRepository userCodeRepository;
    @Autowired
    private TB_PB204Repository TB_PB204Repository;
    @Autowired
    private TbAs010Repository TbAs010Repository;
    @Autowired
    private TbAs010Repository tbAs010Repository;

    //
    @GetMapping("/read")
    public AjaxResult orderStatusRead(
            @RequestParam(value = "startDate", required = false) String startDate,
            @RequestParam(value = "endDate", required = false) String endDate,
            @RequestParam(value = "search_spjangcd", required = false) String searchSpjangcd,
            @RequestParam(required = false) String searchCltnm,
            @RequestParam(required = false) String searchtketnm,
            @RequestParam(required = false) String searchstate,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        log.info("주문 확인 read 들어온 데이터:startDate{}, endDate{}, searchSpjangcd{}, searchCltnm{},searchstate{} ", startDate, endDate, searchSpjangcd, searchCltnm, searchstate);
        try {
            // 로그인한 사용자 정보에서 이름(perid) 가져오기
            User user = (User) auth.getPrincipal();
            String perid = user.getFirst_name(); // 이름을 가져옴
            String spjangcd = searchSpjangcd;

            // 서비스에서 데이터 가져오기
            List<Map<String, Object>> orderStatusList = dashSummaryService.getOrderStatusByOperid(startDate, endDate, perid, spjangcd, searchCltnm, searchtketnm, searchstate);

            // ObjectMapper를 사용하여 hd_files 처리
            ObjectMapper objectMapper = new ObjectMapper();
            for (Map<String, Object> item : orderStatusList) {
                if (item.get("hd_files") != null) {
                    try {
                        // JSON 문자열을 List<Map<String, Object>>로 변환
                        List<Map<String, Object>> fileitems = objectMapper.readValue(
                                (String) item.get("hd_files"),
                                new TypeReference<List<Map<String, Object>>>() {}
                        );

                        // fileitems를 순회하며 필요한 처리 수행
                        for (Map<String, Object> fileitem : fileitems) {
                            if (fileitem.get("filepath") != null && fileitem.get("fileornm") != null) {
                                String filenames = (String) fileitem.get("fileornm");
                                String filepaths = (String) fileitem.get("filepath");
                                String filesvnms = (String) fileitem.get("filesvnm");

                                List<String> fileornmList = filenames != null ? Arrays.asList(filenames.split(",")) : Collections.emptyList();
                                List<String> filepathList = filepaths != null ? Arrays.asList(filepaths.split(",")) : Collections.emptyList();
                                List<String> filesvnmList = filesvnms != null ? Arrays.asList(filesvnms.split(",")) : Collections.emptyList();

                                item.put("isdownload", !fileornmList.isEmpty() && !filepathList.isEmpty());
                            } else {
                                item.put("isdownload", false);
                            }
                        }

                        // 처리된 fileitems를 item에 업데이트
                        item.remove("hd_files");
                        item.put("hd_files", fileitems);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            // AjaxResult 설정
            result.success = true;
            result.data = orderStatusList;
            result.message = "데이터 조회 성공";

        } catch (Exception e) {
            // 오류 발생 시 실패 상태 설정
            result.success = false;
            result.message = "데이터를 가져오는 중 오류가 발생했습니다.";
        }

        return result;
    }

    // 업무일지 목록 조회
    @GetMapping("/read_report")
    public AjaxResult getReportList(
            @RequestParam(value="startDate", required=false) String start_date,
            @RequestParam(value="endDate", required=false) String end_date,
            @RequestParam(value="searchPernm", required=false) String searchPernm,
            @RequestParam(value="search_spjangcd") String spjangcd,
            HttpServletRequest request) {

        if (start_date != null) start_date = start_date.replaceAll("-", "");
        if (end_date != null) end_date = end_date.replaceAll("-", "");

        List<Map<String, Object>> items = this.dashSummaryService.getList(start_date, end_date, searchPernm, spjangcd);

        AjaxResult result = new AjaxResult();
        result.data = items;

        return result;
    }

    @GetMapping("/ModalRead")
    public AjaxResult ModalRead(@RequestParam(required = false) String searchTerm) {
        AjaxResult result = new AjaxResult();

        try {
            List<Map<String, Object>> modalList = dashSummaryService.getModalListByClientName(searchTerm);

            result.success = true;
            result.data = modalList;
            result.message = "데이터 조회 성공";

        } catch (Exception e) {
            // 오류 발생 시 실패 상태 설정
            result.success = false;
            result.message = "데이터를 가져오는 중 오류가 발생했습니다.";
        }

        return result;
    }

    @GetMapping("/searchData")
    public ResponseEntity<Map<String, Object>> searchData(
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate,
            @RequestParam(required = false) String searchCltnm,
            @RequestParam(required = false) String searchtketnm,
            @RequestParam(required = false) String searchstate) {

        // 검색 결과를 서비스에서 가져오기
        List<Map<String, Object>> result = dashSummaryService.searchData(startDate, endDate, searchCltnm, searchtketnm, searchstate);

        // 응답 데이터를 "data" 키로 래핑하여 JSON 형식으로 반환
        Map<String, Object> response = new HashMap<>();
        response.put("data", result);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/getOrdtext")
    public AjaxResult getOrdtext(@RequestParam("reqdate") String reqdate, @RequestParam("remark") String remark) {
        System.out.println("요청사항 팝업 들어옴: reqdate = " + reqdate + ", remark = " + remark);

        AjaxResult result = new AjaxResult();
        try {
            String ordtextData = dashSummaryService.getOrdtextByParams(reqdate, remark);

            result.success = true;
            result.data = ordtextData;
            result.message = "데이터 조회 성공";

        } catch (Exception e) {
            result.success = false;
            result.message = "데이터를 가져오는 중 오류가 발생했습니다.";
            e.printStackTrace(); // 오류 로그 출력
        }

        return result;
    }

    // 근태현황 조회 메서드
    @GetMapping("/readCalenderGrid")
    public AjaxResult getList(@RequestParam(value = "search_spjangcd", required = false) String searchSpjangcd,
                              @RequestParam(value = "search_type", required = false) String searchType,
                              Authentication auth) {
        User user = (User) auth.getPrincipal();
        String username = user.getUsername();  // 유저 사업자번호(id)
        Map<String, Object> userInfo = dashSummaryService.getUserInfo(username);
        List<Map<String, Object>> items = this.dashSummaryService.getOrderList(
                searchSpjangcd, searchType);
        for (Map<String, Object> item : items) {
            // 날짜 형식 변환 (frdate)
            if (item.containsKey("frdate")) {
                String setupdt = (String) item.get("frdate");
                if (setupdt != null && setupdt.length() == 8) {
                    String formattedDate = setupdt.substring(0, 4) + "." + setupdt.substring(4, 6) + "." + setupdt.substring(6, 8);
                    item.put("frdate", formattedDate);
                }
            }
            // 날짜 형식 변환 (todate)
            if (item.containsKey("todate")) {
                String setupdt = (String) item.get("todate");
                if (setupdt != null && setupdt.length() == 8) {
                    String formattedDate = setupdt.substring(0, 4) + "." + setupdt.substring(4, 6) + "." + setupdt.substring(6, 8);
                    item.put("todate", formattedDate);
                }
            }
        }
        AjaxResult result = new AjaxResult();
        result.data = items;

        return result;
    }

//    @GetMapping("/isAdmin")
//    public AjaxResult isAdmin(Authentication auth) {
//        User user = (User) auth.getPrincipal();
//        int userCode = user.getUserProfile().getUserGroup().getId();
//        // 사용자의 권한이 일반거래처(code값 : 35)인지확인
//        String userAuth;
//        if(userCode == 35){
//            userAuth = "nomal";
//        }else {
//            userAuth = "admin";
//        }
//        AjaxResult result = new AjaxResult();
//        result.data = userAuth;
//        return result;
//    }
//
    @GetMapping("/initDatas")
    public AjaxResult initDatas(@RequestParam(value = "search_spjangcd", required = false) String searchSpjangcd,
                                Authentication auth){
        User user = (User) auth.getPrincipal();
        String username = user.getUsername();
        List<Map<String, Object>> items = this.dashSummaryService.initDatas(searchSpjangcd);
        AjaxResult result = new AjaxResult();
        result.data = items;
        return result;
    }

    // 연차정보 조회 메서드(캘린더 바인드)
    @GetMapping("/readCalenderGrid2")
    public AjaxResult getList2(@RequestParam(value = "search_spjangcd", required = false) String searchSpjangcd
            , Authentication auth) {
        User user = (User) auth.getPrincipal();
        String username = user.getUsername();  // 유저 사업자번호(id)
        Map<String, Object> userInfo = dashSummaryService.getUserInfo(username);
        List<Map<String, Object>> items = this.dashSummaryService.getOrderList2();
        for (Map<String, Object> item : items) {

            // 날짜 형식 변환 (frdate)
            if (item.containsKey("frdate")) {
                String setupdt = (String) item.get("frdate");
                if (setupdt != null && setupdt.length() == 8) {
                    String formattedDate = setupdt.substring(0, 4) + "-" + setupdt.substring(4, 6) + "-" + setupdt.substring(6, 8);
                    item.put("frdate", formattedDate);
                }
            }
            // 날짜 형식 변환 (todate)
            if (item.containsKey("todate")) {
                String setupdt = (String) item.get("todate");
                if (setupdt != null && setupdt.length() == 8) {
                    String formattedDate = setupdt.substring(0, 4) + "-" + setupdt.substring(4, 6) + "-" + setupdt.substring(6, 8);
                    item.put("todate", formattedDate);
                }
            }
        }
        AjaxResult result = new AjaxResult();
        result.data = items;

        return result;
    }
//
//    @PostMapping("/confirm")
//    public ResponseEntity<Map<String, Object>> UpdateOrdflag(@RequestBody Map<String, Object> formData) {
//        Map<String, Object> response = new HashMap<>();
//        try {
//            log.info("받은 데이터: {}", formData);
//
//            Object ordersObj = formData.get("orders");
//
//            if (!(ordersObj instanceof List)) {
//                return ResponseEntity.badRequest().body(Map.of("message", "잘못된 데이터 형식입니다. 'orders'는 리스트여야 합니다."));
//            }
//
//            List<Map<String, Object>> orders = (List<Map<String, Object>>) ordersObj;
//
//            if (orders.isEmpty()) {
//                return ResponseEntity.badRequest().body(Map.of("message", "수정할 주문이 없습니다."));
//            }
//
//            // 한글을 숫자로 변환만 수행 (토글 변환 X)
//            List<Map<String, Object>> validOrders = orders.stream()
//                    .map(order -> {
//                        Object ordflagObj = order.get("ordflag");
//                        if (ordflagObj instanceof String) {
//                            String ordflag = (String) ordflagObj;
//                            // 한글 상태를 숫자 문자열로 변환 (변환만 수행, 토글 X)
//                            String ordflagNum = switch (ordflag) {
//                                case "주문등록" -> "0";
//                                case "주문확인" -> "1";
//                                default -> null; // 그 외 값은 필터링
//                            };
//
//                            if (ordflagNum != null) {
//                                order.put("ordflag", ordflagNum); // 변환된 값만 저장
//                            }
//                        }
//                        return order;
//                    })
//                    .filter(order -> {
//                        Object ordflagObj = order.get("ordflag");
//                        return ordflagObj instanceof String && ("0".equals(ordflagObj) || "1".equals(ordflagObj));
//                    })
//                    .collect(Collectors.toList());
//
//            if (validOrders.isEmpty()) {
//                return ResponseEntity.badRequest().body(Map.of("message", "'주문 등록' 과 '주문 확인' 이 외는 수정이 불가능합니다."));
//            }
//
//            // 서비스 호출 (서비스에서 상태 변환 수행)
//            TB_DA006W updateResult = dashSummaryService.UpdateOrdflag(validOrders);
//
//            // 성공 응답 구성
//            response.put("success", true);
//            response.put("message", "주문 상태가 변경되었습니다.");
//            response.put("data", updateResult);
//            log.info("저장 완료: {}", updateResult);
//
//        } catch (Exception e) {
//            log.error("❌ 저장 중 오류 발생", e);
//            response.put("success", false);
//            response.put("message", "저장 중 오류가 발생했습니다. 관리자에게 문의하세요.");
//        }
//
//        return ResponseEntity.ok(response);
//    }
//
//    @PostMapping("/CancelOrder")
//    public ResponseEntity<Map<String, Object>> CancelOrderUpdateOrdflag(@RequestBody Map<String, Object> formData) {
//        Map<String, Object> response = new HashMap<>();
//        try {
//            log.info("📌 주문 확인 취소 요청 데이터: {}", formData);
//
//            Object ordersObj = formData.get("orders");
//
//            // 'orders' 값이 리스트인지 검증
//            if (!(ordersObj instanceof List)) {
//                return ResponseEntity.badRequest().body(Map.of("message", "잘못된 데이터 형식입니다. 'orders'는 리스트여야 합니다."));
//            }
//
//            List<Map<String, Object>> orders = (List<Map<String, Object>>) ordersObj;
//
//            // 주문이 비어있는 경우 처리
//            if (orders.isEmpty()) {
//                return ResponseEntity.badRequest().body(Map.of("message", "수정할 주문이 없습니다."));
//            }
//
//            // "주문등록"(0)과 "주문확인"(1) → "5"로 변환
//            List<Map<String, Object>> validOrders = orders.stream()
//                    .map(order -> {
//                        Object ordflagObj = order.get("ordflag");
//                        if (ordflagObj instanceof String) {
//                            String ordflag = (String) ordflagObj;
//                            if ("주문등록".equals(ordflag) || "주문확인".equals(ordflag)) {
//                                order.put("ordflag", "5");  // 바로 "5"로 변환
//                            }
//                        }
//                        return order;
//                    })
//                    .filter(order -> "5".equals(order.get("ordflag"))) // 변환된 값만 유지
//                    .collect(Collectors.toList());
//
//            // 변환된 주문이 없는 경우 예외 처리
//            if (validOrders.isEmpty()) {
//                return ResponseEntity.badRequest().body(Map.of("message", "'주문 등록'과 '주문 확인' 상태만 수정이 가능합니다."));
//            }
//
//            // 서비스 호출 (상태 업데이트 실행)
//            int updatedCount = dashSummaryService.CancelOrderUpdateOrdflag(validOrders);
//
//            // 성공 응답
//            response.put("success", true);
//            response.put("message", "✅ 주문 상태가 성공적으로 변경되었습니다.");
//            response.put("updatedCount", updatedCount);
//            log.info("✅ 주문 상태 변경 완료 ({}건)", updatedCount);
//
//        } catch (Exception e) {
//            log.error("❌ 저장 중 오류 발생", e);
//            response.put("success", false);
//            response.put("message", "저장 중 오류가 발생했습니다. 관리자에게 문의하세요.");
//        }
//
//        return ResponseEntity.ok(response);
//    }
//
//    @GetMapping("/ordFlagType")
//    public AjaxResult ordFlagType(
//            @RequestParam(value = "parentCode", required = false) String parentCode) {
//        AjaxResult result = new AjaxResult();
//
//        try {
//            // parentCode를 기준으로 하위 그룹 필터링
//            List<UserCode> data = (parentCode != null)
//                    ? userCodeRepository.findByParentId(userCodeRepository.findByCode(parentCode).stream().findFirst().get().getId())
//                    : userCodeRepository.findAll();
//
//            // 성공 시 데이터와 메시지 설정
//            result.success = true;
//            result.message = "데이터 조회 성공";
//            result.data = data;
//
//        } catch (Exception e) {
//            // 예외 발생 시 처리
//            result.success = false;
//            result.message = "데이터 조회 중 오류 발생: " + e.getMessage();
//        }
//
//        return result;
//    }
    // 요청사항 조회
    @GetMapping("/readRequest")
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

        List<Map<String, Object>> searchDatas = dashSummaryService.searchDatas4(
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

    // 월별 유지보수 요청현황 조회
    @GetMapping("/readRequest2")
    public AjaxResult searchDatas2(
            HttpServletRequest request,
//            @RequestParam(value="searchfrdate") String searchfrdate,
//            @RequestParam(value="searchtodate") String searchtodate,
//            @RequestParam(value="searchCompCd", required=false) String searchCompCd,
//            @RequestParam(value="reqType", required=false) String reqType,
//            @RequestParam(value="spjangcd", required=false) String spjangcd,
//            @RequestParam(value="aspernm", required=false) String aspernm,
//            @RequestParam(value="searchCompnm", required=false) String searchCompnm,
//            @RequestParam(value="recyn", required=false) String recyn,
            Authentication auth) {
        AjaxResult result = new AjaxResult();

        User user = (User) auth.getPrincipal();
        Integer perId = user.getPersonid();

        List<Map<String, Object>> searchDatas = dashSummaryService.searchDatas2(
//                searchfrdate
//                , searchtodate
//                , searchCompCd
//                , searchCompnm // 업체명
//                , reqType
//                , spjangcd
//                , perId
//                , recyn
//                , aspernm // 본사담당 이름
        );

        result.data = searchDatas;

        return result;
    }

    // 공지사항 조회
    @GetMapping("/notice")
    public AjaxResult getBoardList(
            @RequestParam(value = "srchStartDt", required = false) String srchStartDt,
            @RequestParam(value = "srchEndDt", required = false) String srchEndDt,
            @RequestParam(value = "keyword", required = false) String keyword,
            HttpServletRequest request) {

        String date_from = srchStartDt + " 00:00:00";
        String date_to = srchEndDt + " 23:59:59";

        List<Map<String, Object>> items = this.dashSummaryService.getBoardList("notice", keyword, date_from, date_to);

        AjaxResult result = new AjaxResult();
        result.data = items;

        return result;
    }
}
