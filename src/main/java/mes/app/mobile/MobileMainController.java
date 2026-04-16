package mes.app.mobile;

import lombok.extern.slf4j.Slf4j;
import mes.app.mobile.Service.MobileMainService;
import mes.app.transaction.service.MonthlyPurchaseListService;
import mes.domain.entity.User;
import mes.domain.entity.commute.TB_PB201;
import mes.domain.entity.commute.TB_PB201_PK;
import mes.domain.entity.mobile.TB_PB204;
import mes.domain.model.AjaxResult;
import mes.domain.repository.commute.TB_PB201Repository;
import mes.domain.repository.mobile.TB_PB204Repository;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@RestController
@RequestMapping("/api/mobile_main")
public class MobileMainController {

    @Autowired
    MobileMainService mobileMainService;

    @Autowired
    private TB_PB201Repository tbPb201Repository;

    @Autowired
    private TB_PB204Repository tbPb204Repository;

    private static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm");

    // 사용자 정보 조회(부서 이름 출근여부)
    @GetMapping("/read_userInfo")
    public AjaxResult getUserInfo(
            HttpServletRequest request,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User)auth.getPrincipal();
        String username = user.getUsername();

        Map<String, Object> userInfo = mobileMainService.getUserInfo(username);
        Map<String, Object> timeInfo = mobileMainService.getInOfficeTime(username);
        Map<String, Object> resultData = new HashMap<>();
        resultData.put("first_name", (String)userInfo.get("first_name"));
        resultData.put("Name", (String)userInfo.get("Name"));
        resultData.put("jik_id", (String)userInfo.get("jik_id"));
        if(timeInfo != null) {
            resultData.put("inOfficeTime", (String) timeInfo.get("starttime"));
            resultData.put("workcd", (String) timeInfo.get("workcd"));
        }

        result.data = resultData;

        return result;
    }

    // 출근 메서드
    @Transactional
    @PostMapping("/submitCommute")
    public AjaxResult submitCommute(
            @RequestParam(value="weekNum") Integer weekNum,
            @RequestParam(value="office") String office,
            @RequestParam(value="workym", required=false) String workym,
            @RequestParam(value="workday", required=false) String workday,
            @RequestParam(value="isHoly", required=false) String isHoly,
            @RequestParam(value="workcd", required=false) String workcd,
            @RequestParam(value="latitude", required=false) String latitude,
            @RequestParam(value="longitude", required=false) String longitude,
            @RequestParam(value="gpsInfo", required=false) String gpsInfo,
            HttpServletRequest request,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        // 일근태 테이블 초기화
        TB_PB201 tbPb201 = new TB_PB201();
        TB_PB201_PK tbPb201Pk = new TB_PB201_PK();

        System.out.println("GPS latitude(위도): " + latitude + ", logitude(경도): " + longitude);

        User user = (User)auth.getPrincipal();
        String username = user.getUsername();
        String spjangcd = user.getSpjangcd();
        // 직원코드 조회 및 근무구분 조회
        Map<String, Object> personInfo = mobileMainService.getPersonId(username);
        String perId = personInfo.get("personid").toString();
        String workType = String.format("%02d", Integer.parseInt(personInfo.get("PersonGroup_id").toString()));
        // 지각여부 확인 (근태설정 비교(jitime(지각)값 설정)) sttime 00:00
        LocalDateTime inOfficeTime = LocalDateTime.now();
        String sttime = (String) mobileMainService.getWorkTime(workType).get("sttime");
        // inOfficeTime에서 시간만 추출 지각 비교
        LocalTime sttimeParsed = LocalTime.parse(sttime, timeFormatter);
        LocalTime currentTime = inOfficeTime.toLocalTime();

        tbPb201Pk.setPersonid(Integer.valueOf(perId));
        tbPb201Pk.setSpjangcd(spjangcd);
        tbPb201Pk.setWorkday(workday);
        tbPb201Pk.setWorkym(workym);

        Optional<TB_PB201> searchtb201 = tbPb201Repository.findById(tbPb201Pk);

        String formattedCurrentTime = currentTime.format(timeFormatter); // "HH:mm" 형식으로 포맷
        String inFlag = "";

        if(searchtb201.isPresent()){
            // 기존 출근데이터가 존재(연차 or 반차로 인한 데이터) - workcd(반차 등) 그대로 유지
            TB_PB201 origin = searchtb201.get();
            // 기존 데이터를 복사
            tbPb201 = origin;
            // 사내 / 외부 출근 확인
            if (office.equals("inOfficeIn")) {
                inFlag = "0";
            } else {
                inFlag = "1";
                tbPb201.setAddress(gpsInfo);
            }
            tbPb201.setJitime(0);
        }else {
            // 기존 출근데이터가 존재x(일반 출근 판단)
            // 기준시간에 +1분 더해서 지각판정
            LocalTime lateTime = sttimeParsed.plusMinutes(1);
            String jitFlag = currentTime.isBefore(lateTime) ? "0" : "1";
            // 사내 / 외부 출근 확인
            if (office.equals("inOfficeIn")) {
                inFlag = "0";
            } else {
                inFlag = "1";
                tbPb201.setWorkcd(workcd);
                tbPb201.setAddress(gpsInfo);
            }
            tbPb201.setJitime(Integer.parseInt(jitFlag));
        }
        tbPb201.setWorknum(weekNum);
        tbPb201.setId(tbPb201Pk);
        tbPb201.setHoliyn(isHoly);
        tbPb201.setStarttime(formattedCurrentTime);
        tbPb201.setInflag(inFlag);

        result.message = "출근등록이 완료되었습니다.";
        try {
            result.data = tbPb201Repository.save(tbPb201);
        }catch (Exception e){
            e.printStackTrace();
            result.message = "오류가 발생하였습니다.";
        }
        return result;
    }
    // 퇴근메서드
    @PostMapping("/modifyCommute")
    public AjaxResult submitCommute(
            @RequestParam(value="office") String office,
            @RequestParam(value="workym", required=false) String workym,
            @RequestParam(value="workday", required=false) String workday,
            @RequestParam(value="remark", required=false) String remark,
            @RequestParam(value="workcd", required=false) String workcd,
            @RequestParam(value="gpsInfo", required=false) String gpsInfo,
            HttpServletRequest request,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User)auth.getPrincipal();
        String username = user.getUsername();
        String spjangcd = user.getSpjangcd();
        // 직원코드 조회 및 근무구분 조회
        Map<String, Object> personInfo = mobileMainService.getPersonId(username);
        String perId = personInfo.get("personid").toString();
        String workType = String.format("%02d", Integer.parseInt(personInfo.get("PersonGroup_id").toString()));

        // 퇴근시간 조회(조퇴 확인) / 근무구분에 따른 정상퇴근시간 조회
        LocalDateTime outOfficeTime = LocalDateTime.now();
        Map<String, Object> WorkTimeInfo = mobileMainService.getWorkTime(workType);
        String endtime = (String) WorkTimeInfo.get("endtime");
        // outOfficeTime 시간만 추출 정상퇴근 비교
        LocalTime endtimeParsed = LocalTime.parse(endtime, timeFormatter);
        LocalTime currentTime = outOfficeTime.toLocalTime();
        String formattedCurrentTime = currentTime.format(timeFormatter); // "HH:mm" 형식으로 포맷

        // 일근태 테이블 초기화
        TB_PB201_PK tbPb201Pk = new TB_PB201_PK();
        tbPb201Pk.setPersonid(Integer.valueOf(perId));
        tbPb201Pk.setSpjangcd(spjangcd);
        tbPb201Pk.setWorkday(workday);
        tbPb201Pk.setWorkym(workym);

        Optional<TB_PB201> savedTbPb201 = tbPb201Repository.findById(tbPb201Pk);
        TB_PB201 entity = savedTbPb201.get();  // 값이 존재하면 꺼냄
        if(entity.getEndtime() != null && !entity.getEndtime().isEmpty()){
            result.message = "이미 퇴근처리 되었습니다.";
            return result;
        }

        // --- 사내/외부 퇴근 및 workcd 처리 ---
        String inFlag = "";
        if ("inOfficeOut".equals(office)) { // 내부 퇴근
            inFlag = "0";
            if (entity.getWorkcd() == null || entity.getWorkcd().isEmpty()) {
                entity.setWorkcd("01");
            }
        } else { // 외부 퇴근
            inFlag = "1";
            if (entity.getWorkcd() == null || entity.getWorkcd().isEmpty()) {
                entity.setWorkcd(workcd); // 프론트에서 받은 workcd(외부코드 등)
                entity.setAddress(gpsInfo);
            }
            // 이미 workcd 있으면 변경하지 않음
        }
        String today = workym + workday;
        // --- 유연근무 신청 여부 확인 ---
        TB_PB204 flexibleWork = tbPb204Repository.findFlexibleWorkByPersonAndDate(
                Integer.valueOf(perId),
                today,
                "13"
        );
        boolean isFlexibleWork = (flexibleWork != null);
        // jotime(조퇴) 판단: 반차("04")/연차("08")면 무조건 0, 그 외엔 정상퇴근시간 이전=1(조퇴), 이후=0(정상)
        boolean isBanchaOrYeoncha = "04".equals(entity.getWorkcd()) || "08".equals(entity.getWorkcd());
        int jotFlag = 0;
        if (isBanchaOrYeoncha) {
            jotFlag = 0;
        } else {
            jotFlag = currentTime.isAfter(endtimeParsed) ? 0 : 1;
        }
        entity.setJotime(jotFlag);

        // workyn(정상근무여부): jittime(지각), jotime(조퇴) 중 하나라도 1이면 "0", 아니면 "1"
        String workyn = (entity.getJitime() == 1 || jotFlag == 1) ? "0" : "1";
        entity.setWorkyn(workyn);
        log.info("saved 201 data : {}", entity);

        // 출근시간 ~ 퇴근시간 비교하여 정상, 연장, 야간 근무시간 계산 후 바인드
        String sttime = (String) WorkTimeInfo.get("sttime"); // 출근시간
        //휴식(점심)시간 설정값으로 할지 몰라 하드코딩
        String startRestTime = "12:00";
        String endRestTime = "13:00";
        String ovsttime = (String) WorkTimeInfo.get("ovsttime"); // 연장근무 시작시간
        String ovedtime = (String) WorkTimeInfo.get("ovedtime"); // 연장근무 종료시간
        String ngsttime = (String) WorkTimeInfo.get("ngsttime"); // 야간근무 시작시간
        String ngedtime = (String) WorkTimeInfo.get("ngedtime"); // 야간근무 종료시간

        // 시간 파싱
        LocalTime startTime = LocalTime.parse(entity.getStarttime(), timeFormatter); // 사용자 출근시간
        LocalTime endTime = currentTime; // 사용자 퇴근시간
        LocalTime normalStart = LocalTime.parse(sttime, timeFormatter);
        LocalTime normalEnd = LocalTime.parse(endtime, timeFormatter); // 정상근무 퇴근시간
        LocalTime overStart = LocalTime.parse(ovsttime, timeFormatter);
        LocalTime overEnd = LocalTime.parse(ovedtime, timeFormatter);
        LocalTime nightStart = LocalTime.parse(ngsttime, timeFormatter);
        LocalTime nightEnd = LocalTime.parse(ngedtime, timeFormatter);
        // 휴식(점심)시간
        LocalTime restStart = LocalTime.parse(startRestTime, timeFormatter);
        LocalTime restEnd = LocalTime.parse(endRestTime, timeFormatter);

        //정상근무 계산
        BigDecimal normalTime = calculateTimeOverlap(startTime, endTime, normalStart, normalEnd, restStart, restEnd);

        // 연장 근무 시간 계산
        BigDecimal overTime = calculateTimeOverlap(startTime, endTime, overStart, overEnd, restStart, restEnd);

        // 야간 근무 시간 계산 (00:00을 기준으로 넘어갈 경우 처리)
        BigDecimal nightTime;
        if (nightEnd.isBefore(nightStart)) {
            // 다음날로 넘어갈 때
            BigDecimal nightPart1 = calculateTimeOverlap(startTime, endTime, nightStart, LocalTime.MAX, restStart, restEnd);
            BigDecimal nightPart2 = calculateTimeOverlap(startTime, endTime, LocalTime.MIN, nightEnd, restStart, restEnd);
            nightTime = nightPart1.add(nightPart2);
        } else {
            nightTime = calculateTimeOverlap(startTime, endTime, nightStart, nightEnd, restStart, restEnd);
        }
        // 총 근무 시간
        BigDecimal totalTime = normalTime.add(overTime).add(nightTime);

        entity.setId(tbPb201Pk);
        entity.setWorkyn(workyn);
        entity.setEndtime(formattedCurrentTime);
        entity.setRemark(remark);
        entity.setInflag(inFlag);
        if (entity.getHoliyn().equals("0")) {
            if (isFlexibleWork) {
                // 🔥 유연근무자 전용 계산 (출퇴근 기준)
                BigDecimal flexibleTime = calculateFlexibleWorkTime(
                        startTime, endTime, restStart, restEnd
                );

                entity.setWorktime(flexibleTime);
                entity.setNomaltime(flexibleTime);
                entity.setOvertime(BigDecimal.ZERO);
                entity.setNighttime(BigDecimal.ZERO);

                log.info("유연근무자 근무시간 계산(출퇴근 기준): {}", flexibleTime);
            } else {
                // 일반 근무자 로직
                entity.setWorktime(totalTime);
                entity.setNomaltime(normalTime);
                entity.setOvertime(overTime);
                entity.setNighttime(nightTime);
            }
        } else {
            // 휴일 근무자 로직
            entity.setWorktime(totalTime);
            entity.setHolitime(totalTime);
        }
        entity.setJotime(jotFlag);
        result.message = "퇴근처리가 마무리되었습니다.";
        try {
            result.data = tbPb201Repository.save(entity);
        } catch (Exception e) {
            throw new RuntimeException(e);

        }
        return result;
    }
    // 유연근무자 근무시간 계산
    private BigDecimal calculateFlexibleWorkTime(
            LocalTime start,
            LocalTime end,
            LocalTime restStart,
            LocalTime restEnd) {

        long totalMinutes;

        // 자정 넘어가는 경우
        if (end.isBefore(start)) {
            totalMinutes =
                    Duration.between(start, LocalTime.MAX).toMinutes()
                            + Duration.between(LocalTime.MIN, end).toMinutes()
                            + 1;
        } else {
            totalMinutes = Duration.between(start, end).toMinutes();
        }

        long restMinutes = calculateRestOverlapMinutes(start, end, restStart, restEnd);

        long workMinutes = Math.max(totalMinutes - restMinutes, 0);

        return BigDecimal.valueOf(workMinutes)
                .divide(BigDecimal.valueOf(60), 2, RoundingMode.DOWN);
    }

    // 휴식시간 계산
    private long calculateRestOverlapMinutes(
            LocalTime start,
            LocalTime end,
            LocalTime restStart,
            LocalTime restEnd) {

        LocalTime overlapStart = start.isAfter(restStart) ? start : restStart;
        LocalTime overlapEnd   = end.isBefore(restEnd) ? end : restEnd;

        if (overlapEnd.isAfter(overlapStart)) {
            return Duration.between(overlapStart, overlapEnd).toMinutes();
        }
        return 0;
    }

    // 좌표 -> 주소 변환 메서드
    @PostMapping("/switchAddress")
    public AjaxResult switchAddress(@RequestParam("lat") String lat,
                                    @RequestParam("lon") String lon) {
        AjaxResult result = new AjaxResult();

        try {
            String apiKey = "782DA4E1-4DC4-3B19-8519-2E2AE8EB2926"; // 공백 주의
            String apiUrl = "https://api.vworld.kr/req/address?service=address&request=getAddress" +
                    "&key=" + apiKey +
                    "&format=json&type=both&crs=epsg:4326&point=" + lon + "," + lat;


            RestTemplate restTemplate = new RestTemplate();
            String response = restTemplate.getForObject(apiUrl, String.class);
            //System.out.println("VWorld response: " + response);

            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(response);

            JsonNode responseObj = root.path("response");
            JsonNode resultArray = responseObj.path("result");

            if (resultArray.isArray() && !resultArray.isEmpty()) {
                String address = resultArray.get(0).path("text").asText();
                result.success = true;
                result.message = "주소 변환 성공";
                result.data = address;
            } else {
                result.success = false;
                result.message = "주소를 찾을 수 없습니다.";
            }

        } catch (Exception e) {
            result.success=false;
            result.message="API 호출 오류: " + e.getMessage();
        }

        return result;
    }


    // 시간대별 근무시간 계산 메서드 (휴식 시간 제외)
    public static BigDecimal calculateTimeOverlap(LocalTime start, LocalTime end, LocalTime rangeStart, LocalTime rangeEnd, LocalTime restStart, LocalTime restEnd) {
        // 시간대 겹침 계산
        LocalTime actualStart = start.isBefore(rangeStart) ? rangeStart : start;
        LocalTime actualEnd = end.isAfter(rangeEnd) ? rangeEnd : end;

        // 정상 근무 시간이 있을 경우에만 처리
        if (actualStart.isBefore(actualEnd)) {
            // 휴식 시간 체크
            Duration workDuration = Duration.between(actualStart, actualEnd);

            // 근무 시간이 휴식 시간과 겹치는 경우
            if (!(restEnd.isBefore(actualStart) || restStart.isAfter(actualEnd))) {
                // 겹치는 시간 계산
                LocalTime restOverlapStart = actualStart.isBefore(restStart) ? restStart : actualStart;
                LocalTime restOverlapEnd = actualEnd.isAfter(restEnd) ? restEnd : actualEnd;

                if (restOverlapStart.isBefore(restOverlapEnd)) {
                    Duration restDuration = Duration.between(restOverlapStart, restOverlapEnd);
                    workDuration = workDuration.minus(restDuration);
                }
            }

            // 소수점 2자리까지 반올림
            double hours = workDuration.toMinutes() / 60.0;
            double intPart = Math.floor(hours);
            double fractional = hours - intPart;

            double adjusted;
            if (fractional == 0.0 || fractional == 0.5) {
                adjusted = intPart + fractional;
            } else if (fractional < 0.5) {
                adjusted = intPart;
            } else {
                adjusted = intPart + 0.5;
            }

            return BigDecimal.valueOf(adjusted).setScale(1, RoundingMode.HALF_UP);
        }
        return BigDecimal.ZERO;
    }
}
