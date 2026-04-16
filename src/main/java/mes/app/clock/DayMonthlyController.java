package mes.app.clock;

import mes.app.clock.service.DayMonthlyService;
import mes.domain.entity.Tb_pb203;
import mes.domain.entity.Tb_pb203Id;
import mes.domain.entity.User;
import mes.domain.entity.Yearamt;
import mes.domain.entity.commute.TB_PB201;
import mes.domain.entity.commute.TB_PB201_PK;
import mes.domain.model.AjaxResult;
import mes.domain.repository.Tb_pb203Repository;
import mes.domain.repository.commute.TB_PB201Repository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;
import java.math.BigDecimal;
import java.util.*;

@RestController
@RequestMapping("/api/clock/DayMonthly")
public class DayMonthlyController {

    @Autowired
    private DayMonthlyService dayMonthlyService;
    @Autowired
    private TB_PB201Repository tbPb201Repository;

    @Autowired
    private Tb_pb203Repository tb_pb203Repository;



    @GetMapping("/read")
    public AjaxResult getDayList(
            @RequestParam(value="work_division", required=false) String work_division,
            @RequestParam(value="serchday", required=false) String serchday,
            @RequestParam(value="depart", required=false) String depart,
            @RequestParam(value ="spjangcd") String spjangcd,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();

        if (serchday != null && serchday.contains("-")) {
            serchday = serchday.replaceAll("-", "");
        }

        List<Map<String, Object>> items = this.dayMonthlyService.getDayList(work_division, serchday,spjangcd,depart);
        result.data = items;
        return result;
    }


    @PostMapping("/savedata")
    @Transactional
    public AjaxResult saveDayDataList(
            @RequestBody Map<String, Object> requestData,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();
        User user = (User)auth.getPrincipal();

        List<Map<String, Object>> dataList = (List<Map<String, Object>>) requestData.get("list");
        String spjangcd = (String) requestData.get("spjangcd");

        if (dataList == null || dataList.isEmpty()) {
            result.success=false;
            result.message="저장할 데이터가 없습니다.";
            return result;
        }

        List<TB_PB201> tbpb201List = new ArrayList<>();

        for (Map<String, Object> item : dataList) {
            String workymd = (String) item.get("workymd");
            String workym = workymd.substring(0, 4) + workymd.substring(5, 7); // 202505
            String workday = workymd.substring(8, 10); // 28

            Integer id = ((Number) item.get("id")).intValue();
            String workcd = (String) item.get("workcd");

            String starttimeStr = (String) item.get("starttime"); // "09:30"
            String endtimeStr = (String) item.get("endtime");
            Object nomaltimeStr = item.get("nomaltime");
            Object jitimeStr = item.get("jitime");
            Object overtimeStr = item.get("overtime");
            Object nighttimeStr = item.get("nighttime");
            Object yuntimeStr =  item.get("yuntime");
            Object abtimeStr = item.get("abtime");
            Object holitimeStr = item.get("holitime");
            Object worktimeStr = item.get("worktime");

            String address= (String) item.get("address");


            Optional<TB_PB201> optional = tbPb201Repository.findByIdSpjangcdAndIdWorkymAndIdWorkdayAndIdPersonid(spjangcd,workym, workday, id);
            TB_PB201 tbpb201;


            if (optional.isPresent()) {
                tbpb201 = optional.get(); // 기존 데이터 업데이트
            }else {
                tbpb201 = new TB_PB201();
                TB_PB201_PK pk = new TB_PB201_PK();
                pk.setSpjangcd(spjangcd);
                pk.setWorkym(workym);
                pk.setWorkday(workday);
                pk.setPersonid(id);
                tbpb201.setId(pk);
                tbpb201.setFixflag("0");
            }

                tbpb201.setWorkcd(workcd);
                tbpb201.setAddress(address);

                if (starttimeStr != null && !starttimeStr.trim().isEmpty()) {
                    // "HH:mm" 형식인지 간단한 유효성 검사
                    if (starttimeStr.matches("^\\d{2}:\\d{2}$")) {
                        tbpb201.setStarttime(starttimeStr.trim());
                    } else {
                        result.success = false;
                        result.message = "출근시간 형식이 올바르지 않습니다. (예: 09:30)";
                        return result;
                    }
                }

                if (endtimeStr != null && !endtimeStr.trim().isEmpty()) {
                    // "HH:mm" 형식인지 간단한 유효성 검사
                    if (endtimeStr.matches("^\\d{2}:\\d{2}$")) {
                        tbpb201.setEndtime(endtimeStr.trim());
                    } else {
                        result.success = false;
                        result.message = "퇴근시간 형식이 올바르지 않습니다. (예: 09:30)";
                        return result;
                    }
                }

                if (nomaltimeStr != null ) {
                    try {
                        BigDecimal nomaltime = new BigDecimal(nomaltimeStr.toString());
                        tbpb201.setNomaltime(nomaltime);
                    } catch (NumberFormatException e) {
                        result.success = false;
                        result.message = "근무시간 값이 숫자 형식이 아닙니다: " + nomaltimeStr;
                        return result;
                    }
                }

                if (jitimeStr != null ) {
                    try {
                        int jitime = Integer.parseInt(jitimeStr.toString());
                        tbpb201.setJitime(jitime);
                    } catch (NumberFormatException e) {
                        result.success = false;
                        result.message = "지각 값이 숫자 형식이 아닙니다: " + jitimeStr;
                        return result;
                    }
                }

                if (overtimeStr != null ) {
                    try {
                        BigDecimal overtime = new BigDecimal(overtimeStr.toString());
                        tbpb201.setOvertime(overtime);
                    } catch (NumberFormatException e) {
                        result.success = false;
                        result.message = "연장근무 값이 숫자 형식이 아닙니다: " + overtimeStr;
                        return result;
                    }
                }

                if (nighttimeStr != null) {
                    try {
                        BigDecimal nighttime = new BigDecimal(nighttimeStr.toString());
                        tbpb201.setNighttime(nighttime);
                    } catch (NumberFormatException e) {
                        result.success = false;
                        result.message = "야간근무 값이 숫자 형식이 아닙니다: " + nighttimeStr;
                        return result;
                    }
                }
                
                if (yuntimeStr != null ) {
                    try {
                        int yuntime = Integer.parseInt(yuntimeStr.toString());
                        tbpb201.setYuntime(yuntime);
                    } catch (NumberFormatException e) {
                        result.success = false;
                        result.message = "연차 값이 숫자 형식이 아닙니다: " + yuntimeStr;
                        return result;
                    }
                }

                if (abtimeStr != null ) {
                    try {
                        int abtime = Integer.parseInt(abtimeStr.toString());
                        tbpb201.setAbtime(abtime);
                    } catch (NumberFormatException e) {
                        result.success = false;
                        result.message = "지각 값이 숫자 형식이 아닙니다: " + abtimeStr;
                        return result;
                    }
                }


                if (holitimeStr != null ) {
                    try {
                        BigDecimal holitime = new BigDecimal(holitimeStr.toString());
                        tbpb201.setHolitime(holitime);
                    } catch (NumberFormatException e) {
                        result.success = false;
                        result.message = "지각 값이 숫자 형식이 아닙니다: " + holitimeStr;
                        return result;
                    }
                }


                if (worktimeStr != null ) {
                    try {
                        BigDecimal worktime = new BigDecimal(worktimeStr.toString());
                        tbpb201.setWorktime(worktime);
                    } catch (NumberFormatException e) {
                        result.success = false;
                        result.message = "총근무시간 값이 숫자 형식이 아닙니다: " + worktimeStr;
                        return result;
                    }
                }
                tbpb201List.add(tbpb201);
            }


        // 저장
        List<TB_PB201> savedList = tbPb201Repository.saveAll(tbpb201List);

        result.success = true;
        result.data = savedList;
        return result;
    }


    @PostMapping("/save")
    @Transactional
    public AjaxResult saveDayList(
            @RequestBody Map<String, Object> requestData,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();
        User user = (User)auth.getPrincipal();

        List<Map<String, Object>> dataList = (List<Map<String, Object>>) requestData.get("list");
        String spjangcd = (String) requestData.get("spjangcd");

        if (dataList == null || dataList.isEmpty()) {
            result.success=false;
            result.message="저장할 데이터가 없습니다.";
            return result;
        }

        List<TB_PB201> tbpb201List = new ArrayList<>();

        for (Map<String, Object> item : dataList) {
            String workym = (String) item.get("workym");
            String workday = (String) item.get("workday");
            Integer personid = ((Number) item.get("id")).intValue();
            String workcd = (String) item.get("workcd");

            String starttimeStr = (String) item.get("starttime"); // "09:30"
            String endtimeStr = (String) item.get("endtime");
            Object nomaltimeStr = item.get("nomaltime");
            Object jitimeStr = item.get("jitime");
            Object overtimeStr = item.get("overtime");
            Object nighttimeStr = item.get("nighttime");
            Object yuntimeStr =  item.get("yuntime");
            Object abtimeStr = item.get("abtime");
            Object holitimeStr = item.get("holitime");
            Object worktimeStr = item.get("worktime");

            String address= (String) item.get("address");


            Optional<TB_PB201> optional = tbPb201Repository.findByIdSpjangcdAndIdWorkymAndIdWorkdayAndIdPersonid(spjangcd,workym, workday, personid);


            if (optional.isPresent()) {
                TB_PB201 tbpb201 = optional.get();
                tbpb201.setFixflag("1");
                tbpb201.setWorkcd(workcd);
                tbpb201.setAddress(address);

                if (starttimeStr != null && !starttimeStr.trim().isEmpty()) {
                    // "HH:mm" 형식인지 간단한 유효성 검사
                    if (starttimeStr.matches("^\\d{2}:\\d{2}$")) {
                        tbpb201.setStarttime(starttimeStr.trim());
                    } else {
                        result.success = false;
                        result.message = "출근시간 형식이 올바르지 않습니다. (예: 09:30)";
                        return result;
                    }
                }

                if (endtimeStr != null && !endtimeStr.trim().isEmpty()) {
                    // "HH:mm" 형식인지 간단한 유효성 검사
                    if (endtimeStr.matches("^\\d{2}:\\d{2}$")) {
                        tbpb201.setEndtime(endtimeStr.trim());
                    } else {
                        result.success = false;
                        result.message = "퇴근시간 형식이 올바르지 않습니다. (예: 09:30)";
                        return result;
                    }
                }

                if (nomaltimeStr != null ) {
                    try {
                        BigDecimal nomaltime = new BigDecimal(nomaltimeStr.toString());
                        tbpb201.setNomaltime(nomaltime);
                    } catch (NumberFormatException e) {
                        result.success = false;
                        result.message = "근무시간 값이 숫자 형식이 아닙니다: " + nomaltimeStr;
                        return result;
                    }
                }

                if (jitimeStr != null ) {
                    try {
                        int jitime = Integer.parseInt(jitimeStr.toString());
                        tbpb201.setJitime(jitime);
                    } catch (NumberFormatException e) {
                        result.success = false;
                        result.message = "지각 값이 숫자 형식이 아닙니다: " + jitimeStr;
                        return result;
                    }
                }

                if (overtimeStr != null ) {
                    try {
                        BigDecimal overtime = new BigDecimal(overtimeStr.toString());
                        tbpb201.setOvertime(overtime);
                    } catch (NumberFormatException e) {
                        result.success = false;
                        result.message = "연장근무 값이 숫자 형식이 아닙니다: " + overtimeStr;
                        return result;
                    }
                }

                if (nighttimeStr != null) {
                    try {
                        BigDecimal nighttime = new BigDecimal(nighttimeStr.toString());
                        tbpb201.setNighttime(nighttime);
                    } catch (NumberFormatException e) {
                        result.success = false;
                        result.message = "야간근무 값이 숫자 형식이 아닙니다: " + nighttimeStr;
                        return result;
                    }
                }

                if (yuntimeStr != null ) {
                    try {
                        int yuntime = Integer.parseInt(yuntimeStr.toString());
                        tbpb201.setYuntime(yuntime);
                    } catch (NumberFormatException e) {
                        result.success = false;
                        result.message = "연차 값이 숫자 형식이 아닙니다: " + yuntimeStr;
                        return result;
                    }
                }

                if (abtimeStr != null ) {
                    try {
                        int abtime = Integer.parseInt(abtimeStr.toString());
                        tbpb201.setAbtime(abtime);
                    } catch (NumberFormatException e) {
                        result.success = false;
                        result.message = "지각 값이 숫자 형식이 아닙니다: " + abtimeStr;
                        return result;
                    }
                }


                if (holitimeStr != null ) {
                    try {
                        BigDecimal holitime = new BigDecimal(holitimeStr.toString());
                        tbpb201.setHolitime(holitime);
                    } catch (NumberFormatException e) {
                        result.success = false;
                        result.message = "지각 값이 숫자 형식이 아닙니다: " + holitimeStr;
                        return result;
                    }
                }


                if (worktimeStr != null ) {
                    try {
                        BigDecimal worktime = new BigDecimal(worktimeStr.toString());
                        tbpb201.setWorktime(worktime);
                    } catch (NumberFormatException e) {
                        result.success = false;
                        result.message = "총근무시간 값이 숫자 형식이 아닙니다: " + worktimeStr;
                        return result;
                    }
                }
                tbpb201List.add(tbpb201);
            }
        }

        // 저장
        List<TB_PB201> savedList = tbPb201Repository.saveAll(tbpb201List);

        result.success = true;
        result.data = savedList;
        return result;
    }


    @PostMapping("/MagamCancel")
    @Transactional
    public AjaxResult DayMagamCancel(
            @RequestBody Map<String, Object> requestData,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();
        List<Map<String, Object>> dataList = (List<Map<String, Object>>) requestData.get("list");

        for (Map<String, Object> item : dataList) {
            String workym = (String) item.get("workym"); // 년월
            String spjangcd = (String) item.get("spjangcd"); // 사업장코드
            String workday = (String) item.get("workday");
            Integer personid = ((Number) item.get("id")).intValue(); // 사번

            TB_PB201_PK id = new TB_PB201_PK(spjangcd, workym, workday, personid);

            Optional<TB_PB201> optional = tbPb201Repository.findById(id);
            if (optional.isPresent()) {
                TB_PB201 entity = optional.get();
                entity.setFixflag("0"); // fixflag를 "0"으로 설정
                tbPb201Repository.save(entity); // 변경사항 저장
            }
        }

        result.success = true;
        return result;
    }




    @PostMapping("workcdList")
    public AjaxResult getspjangcd(@RequestParam(value ="spjangcd") String spjangcd){

        AjaxResult result = new AjaxResult();

        List<Map<String, String>> list = dayMonthlyService.workcdList(spjangcd);

        result.data = list;
        return result;
    }



/*월정산 Read */
    @GetMapping("/MonthlyRead")
    public AjaxResult getMonthlyRead(
            @RequestParam(value="person_name", required=false) String person_name,
            @RequestParam(value="startdate", required=false) String startdate,
            @RequestParam(value="depart", required=false) String depart,
            @RequestParam(value ="spjangcd") String spjangcd,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();

        if (startdate != null && startdate.contains("-")) {
            startdate = startdate.replaceAll("-", "");
        }

        List<Map<String, Object>> items = this.dayMonthlyService.getMonthlyReadList(person_name, startdate,spjangcd,depart);
        result.data = items;
        return result;
    }


/* 월정산 버튼 클릭시 동작 */
    @GetMapping("/getMonthlyList")
    public AjaxResult getMonthlyList(
            @RequestParam(value = "startdate", required = false) String startdate,
            @RequestParam(value = "spjangcd") String spjangcd) {

        AjaxResult result = new AjaxResult();

        if (startdate != null && startdate.contains("-")) {
            startdate = startdate.replaceAll("-", "");
        }

        int insertCount = this.dayMonthlyService.insertWorkSummary(spjangcd, startdate);

        Map<String, Object> responseData = new HashMap<>();
        responseData.put("insertCount", insertCount);

        result.data = responseData;

        if (insertCount == 0) {
            result.success = false;
            result.message = "일별마감 데이터가 없습니다.";
        } else {
            result.success = true;
        }

        return result;
    }


    @PostMapping("/MonthlysaveMagam")
    @Transactional
    public AjaxResult saveMonthlyMagamList(
            @RequestBody Map<String, Object> requestData,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();
        User user = (User)auth.getPrincipal();

        List<Map<String, Object>> dataList = (List<Map<String, Object>>) requestData.get("list");

        if (dataList == null || dataList.isEmpty()) {
            result.success=false;
            result.message="저장할 데이터가 없습니다.";
            return result;
        }

        List<Tb_pb203> tbpb203List = new ArrayList<>();

        for (Map<String, Object> item : dataList) {
            String workym = (String) item.get("workym"); //년월
            Object workdayStr = item.get("workday"); //근무일수
            String spjangcd = (String) item.get("spjangcd");

            Integer personid = ((Number) item.get("personid")).intValue(); // 사번

            Object nomaltimeStr = item.get("nomaltime");
            Object worktimeStr = item.get("worktime");
            Object jitimeStr = item.get("jitime");
            Object jotimeStr = item.get("jotime");
            Object overtimeStr = item.get("overtime");
            Object nighttimeStr = item.get("nighttime");

            Object yuntimeStr =  item.get("yuntime");
            Object abtimeStr = item.get("abtime");
            Object holitimeStr = item.get("holitime");

            Optional<Tb_pb203> optional = tb_pb203Repository.findByIdSpjangcdAndIdWorkymAndIdPersonid(spjangcd,workym, personid);

            if (optional.isPresent()) {
            Tb_pb203 tbpb203 = optional.get();


            tbpb203.setFixflag("1");

            // 근무일수
            if (workdayStr != null ) {
                try {
                    int workday = Integer.parseInt(workdayStr.toString());
                    tbpb203.setWorkday(workday);
                } catch (NumberFormatException e) {
                    result.success = false;
                    result.message = "근무일수 값이 숫자 형식이 아닙니다: " + workdayStr;
                    return result;
                }
            }

            // 정상근무시간
            if (nomaltimeStr != null ) {
                try {
                    BigDecimal nomaltime = new BigDecimal(nomaltimeStr.toString());
                    tbpb203.setNomaltime(nomaltime);
                } catch (NumberFormatException e) {
                    result.success = false;
                    result.message = "근무시간 값이 숫자 형식이 아닙니다: " + nomaltimeStr;
                    return result;
                }
            }

            // 총근무시간
            if (worktimeStr != null ) {
                try {
                    BigDecimal worktime = new BigDecimal(worktimeStr.toString());
                    tbpb203.setWorktime(worktime);
                } catch (NumberFormatException e) {
                    result.success = false;
                    result.message = "총근무시간 값이 숫자 형식이 아닙니다: " + worktimeStr;
                    return result;
                }
            }

            // 지각
            if (jitimeStr != null ) {
                try {
                    BigDecimal jitime = new BigDecimal(jitimeStr.toString());
                    tbpb203.setJitime(jitime);
                } catch (NumberFormatException e) {
                    result.success = false;
                    result.message = "지각 값이 숫자 형식이 아닙니다: " + jitimeStr;
                    return result;
                }
            }

            // 조퇴
            if (jotimeStr != null ) {
                try {
                    BigDecimal jotime = new BigDecimal(jitimeStr.toString());
                    tbpb203.setJitime(jotime);
                } catch (NumberFormatException e) {
                    result.success = false;
                    result.message = "지각 값이 숫자 형식이 아닙니다: " + jitimeStr;
                    return result;
                }
            }

            // 연장
            if (overtimeStr != null ) {
                try {
                    BigDecimal overtime = new BigDecimal(overtimeStr.toString());
                    tbpb203.setOvertime(overtime);
                } catch (NumberFormatException e) {
                    result.success = false;
                    result.message = "연장근무 값이 숫자 형식이 아닙니다: " + overtimeStr;
                    return result;
                }
            }

            // 야간
            if (nighttimeStr != null) {
                try {
                    BigDecimal nighttime = new BigDecimal(nighttimeStr.toString());
                    tbpb203.setNighttime(nighttime);
                } catch (NumberFormatException e) {
                    result.success = false;
                    result.message = "야간근무 값이 숫자 형식이 아닙니다: " + nighttimeStr;
                    return result;
                }
            }

            // 휴가
            if (yuntimeStr != null ) {
                try {
                    BigDecimal yuntime = new BigDecimal(yuntimeStr.toString());
                    tbpb203.setYuntime(yuntime);
                } catch (NumberFormatException e) {
                    result.success = false;
                    result.message = "휴가 값이 숫자 형식이 아닙니다: " + yuntimeStr;
                    return result;
                }
            }

            //결근
            if (abtimeStr != null ) {
                try {
                    BigDecimal abtime = new BigDecimal(abtimeStr.toString());
                    tbpb203.setAbtime(abtime);
                } catch (NumberFormatException e) {
                    result.success = false;
                    result.message = "결근 값이 숫자 형식이 아닙니다: " + abtimeStr;
                    return result;
                }
            }

            // 특근
            if (holitimeStr != null ) {
                try {
                    BigDecimal holitime = new BigDecimal(holitimeStr.toString());
                    tbpb203.setHolitime(holitime);
                } catch (NumberFormatException e) {
                    result.success = false;
                    result.message = "특근 값이 숫자 형식이 아닙니다: " + holitimeStr;
                    return result;
                }
            }

            tbpb203List.add(tbpb203);
            }
        }

        // 저장
        List<Tb_pb203> savedList = tb_pb203Repository.saveAll(tbpb203List);

        result.success = true;
        result.data = savedList;
        return result;
    }


    @PostMapping("/delete")
    @Transactional
    public AjaxResult deleteMonthlyList(
            @RequestBody Map<String, Object> requestData,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();
        List<Map<String, Object>> dataList = (List<Map<String, Object>>) requestData.get("list");

        for (Map<String, Object> item : dataList) {
            String workym = (String) item.get("workym"); // 년월
            String spjangcd = (String) item.get("spjangcd"); // 사업장코드
            Integer personid = ((Number) item.get("personid")).intValue(); // 사번

            // 복합키 생성
            Tb_pb203Id id = new Tb_pb203Id(spjangcd, workym, personid);

            // 삭제
            tb_pb203Repository.deleteById(id);
        }

        result.success=true;
        return result;
    }

    @PostMapping("/MonthlyCancelMagam")
    @Transactional
    public AjaxResult CancelMonthlyList(
            @RequestBody Map<String, Object> requestData,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();
        List<Map<String, Object>> dataList = (List<Map<String, Object>>) requestData.get("list");

        for (Map<String, Object> item : dataList) {
            String workym = (String) item.get("workym"); // 년월
            String spjangcd = (String) item.get("spjangcd"); // 사업장코드
            Integer personid = ((Number) item.get("personid")).intValue(); // 사번

            Tb_pb203Id id = new Tb_pb203Id(spjangcd, workym, personid);

            Optional<Tb_pb203> optional = tb_pb203Repository.findById(id);
            if (optional.isPresent()) {
                Tb_pb203 entity = optional.get();
                entity.setFixflag("0"); // fixflag를 "0"으로 설정
                tb_pb203Repository.save(entity); // 변경사항 저장
            }
        }

        result.success = true;
        return result;
    }


    @PostMapping("/deletedata")
    @Transactional
    public AjaxResult deleteDataList(
            @RequestBody Map<String, Object> requestData,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();
        User user = (User)auth.getPrincipal();

        List<Map<String, Object>> dataList = (List<Map<String, Object>>) requestData.get("list");
        String spjangcd = (String) requestData.get("spjangcd");

        if (dataList == null || dataList.isEmpty()) {
            result.success=false;
            result.message="삭제할 데이터가 없습니다.";
            return result;
        }
        int deleteCount = 0;

        List<TB_PB201> tbpb201List = new ArrayList<>();

        for (Map<String, Object> item : dataList) {
            Object fixflagObj = item.get("fixflag");
            String fixflag = fixflagObj != null ? String.valueOf(fixflagObj) : null;

            // fixflag가 "0" 또는 null일 때만 삭제
            if (fixflag == null || "0".equals(fixflag) || "null".equalsIgnoreCase(fixflag)) {
                String workymd = (String) item.get("workymd");
                Object idObj = item.get("id");
                if (workymd == null || workymd.length() < 10 || idObj == null) {
                    // 필수값 누락된 경우, 다음 item으로 skip
                    continue;
                }
                String workym = workymd.substring(0, 4) + workymd.substring(5, 7);
                String workday = workymd.substring(8, 10);

                Integer id;
                try {
                    id = ((Number) idObj).intValue();
                } catch (Exception e) {
                    continue; // 숫자 변환 실패시 skip
                }

                TB_PB201_PK pk = new TB_PB201_PK();
                pk.setSpjangcd(spjangcd);
                pk.setWorkym(workym);
                pk.setWorkday(workday);
                pk.setPersonid(id);

                if(tbPb201Repository.existsById(pk)) {
                    tbPb201Repository.deleteById(pk);
                    deleteCount++;
                }
            }
        }


        if (deleteCount > 0) {
            result.success = true;
            result.message = deleteCount + "건이 삭제되었습니다.";
        } else {
            result.success = false;
            result.message = "삭제할 데이터가 없습니다. \n(마감된 데이터는 삭제되지 않습니다)";
        }

        return result;
    }

    // =========================================================
    // 사원별 월단위 일일근태 현황 조회
    // =========================================================
    @GetMapping("/personMonthDaily")
    public AjaxResult personMonthDaily(
            @RequestParam(value = "startdate",      required = false) String startdate,
            @RequestParam(value = "personid",        required = false) Integer personid,
            @RequestParam(value = "spjangcd")                        String spjangcd,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();

        if (startdate != null && startdate.contains("-")) {
            startdate = startdate.replaceAll("-", "");
        }

        // ✅ Service에서 idx별 전체 레코드 반환 (일반 + 추가근무 모두 포함)
        List<Map<String, Object>> items = this.dayMonthlyService.getPersonMonthDaily(startdate, personid, spjangcd);
        result.data = items;
        return result;
    }

}
