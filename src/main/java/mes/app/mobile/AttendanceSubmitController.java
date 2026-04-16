package mes.app.mobile;

import lombok.extern.slf4j.Slf4j;
import mes.app.mobile.Service.AttendanceSubmitService;
import mes.app.mobile.Service.MobileMainService;
import mes.domain.entity.User;
import mes.domain.entity.approval.TB_E080;
import mes.domain.entity.approval.TB_E080_PK;
import mes.domain.entity.commute.TB_PB201;
import mes.domain.entity.commute.TB_PB201_PK;
import mes.domain.entity.mobile.TB_PB204;
import mes.domain.model.AjaxResult;
import mes.domain.repository.approval.E080Repository;
import mes.domain.repository.mobile.TB_PB204Repository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@Transactional
@RequestMapping("/api/attendance_submit")
public class AttendanceSubmitController {
    @Autowired
    AttendanceSubmitService attendanceSubmitService;
    @Autowired
    TB_PB204Repository tbPb204Repository;
    @Autowired
    E080Repository e080Repository;
    // 사용자 정보 조회(부서 이름 출근여부)
    @GetMapping("/read_userInfo")
    public AjaxResult getUserInfo(
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();
        User user = (User)auth.getPrincipal();
        String username = user.getUsername();

        Map<String, Object> resultData = attendanceSubmitService.getUserInfo(username);

        result.data = resultData;

        return result;
    }
    // 휴가등록 메서드
    @PostMapping("/submitAttendance")
    public AjaxResult submitCommute(
            @RequestParam(value="userId") String userId,
            @RequestParam(value="userName") String userName,
            @RequestParam(value="attKind", required=false) String attKind,
            @RequestParam(value="startDate", required=false) String startDate,
            @RequestParam(value="startTime", required=false) String startTime,
            @RequestParam(value="endDate", required=false) String endDate,
            @RequestParam(value="endTime", required=false) String endTime,
            @RequestParam(value="isAnnual", required=false) String isAnnual,
            @RequestParam(value="useDate", required=false) BigDecimal useDate,
            @RequestParam(value="usedDate", required=false) String usedDate,
            @RequestParam(value="remark", required=false) String remark,
            HttpServletRequest request,
            Authentication auth) {
        System.out.println("request Data : " + userId);
        AjaxResult result = new AjaxResult();
        // 휴가관리 테이블 초기화
        TB_PB204 tbPb204 = new TB_PB204();

        User user = (User)auth.getPrincipal();
        String username = user.getUsername();
        String spjangcd = user.getSpjangcd();
        String reqdate = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String formattedStartDate = startDate.replaceAll("-", ""); // "20250505"
        String formattedEndDate = endDate.replaceAll("-", "");

        tbPb204.setSpjangcd(spjangcd); // 사업장코드
        tbPb204.setReqdate(reqdate); // 신청일자
        tbPb204.setPersonid(user.getPersonid()); // 직원코드 personid

        tbPb204.setFrdate(formattedStartDate);
        tbPb204.setSttime(startTime); // 시작시간
        tbPb204.setTodate(formattedEndDate);  // 종료일자
        tbPb204.setEdtime(endTime);   // 종료시간
        tbPb204.setDaynum(useDate); // 휴가기간
        tbPb204.setWorkcd(attKind); // 휴가구분
        tbPb204.setRemark(remark); // 휴가 사유
        tbPb204.setAppdate(reqdate); // 결재상신일자
        TB_PB204 saved204 = tbPb204Repository.save(tbPb204);
        String savedId = String.format("%08d", saved204.getId()); // 8자리로 앞에 0을 채움
        tbPb204.setAppnum(reqdate + savedId + spjangcd); // 결재번호 (현재날자(신청일자) + 휴가ID + spjangcd)
        // 결재구분
        tbPb204.setAppgubun("001"); // 결재구분 (001 = 결재대기)
        tbPb204.setAppperid(user.getPersonid()); // 결재상신사원 (personid)
        tbPb204.setAppuserid(user.getUsername()); // 결재상신아이디
        tbPb204.setYearflag(isAnnual);
        // 결재테이블 insert
        // 결재구분별 결재라인 및 상신사원 조회(문서구분 301 / 휴가신청서)
        List<Map<String, Object>> appInfo = attendanceSubmitService.getAppInfoList(user.getPersonid());

        int index = 0;
        for(Map<String, Object> appInfoDetail : appInfo){
            TB_E080 e080Info = new TB_E080();
            TB_E080_PK e080PK = new TB_E080_PK();
            e080PK.setSpjangcd(spjangcd);// 사업장코드
            e080PK.setAppnum(reqdate + savedId + spjangcd); // 결재번호
            e080PK.setPersonid((Integer) appInfoDetail.get("kcpersonid"));// 결재할 사원아이디
            e080PK.setSeq(String.format("%03d", index + 1)); // 순번
            e080Info.setId(e080PK);
            e080Info.setTitle("휴가신청서");
            if(index == 0) {
                e080Info.setFlag("1");// 결재할라인구분(결재상신받는 사람만 1로)
                e080Info.setRepoperid(user.getPersonid());// 결재상신사원아이디(결재라인에따라 개별 등록)
                e080Info.setAppgubun("001");// 결재상태구분 (001 = 결재대기)
            }else {
                e080Info.setFlag("0");
            }
//            e080Info.setRepodate(reqdate);// 결재상신일자
            e080Info.setPapercd("301");// 결재문서구분 (301 휴가신첟서)
            e080Info.setInperid(user.getPersonid());// 등록사원아이디
            e080Info.setIndate(reqdate);// 등록일자
//            e080Info.setAdflag();// 첨부문서구분
//            e080Info.setPrtflag();// 보고양식문서
            e080Info.setGubun((String) appInfoDetail.get("gubun"));// 결재구분
            index++; // 인덱스 증가

            e080Repository.save(e080Info);
        }
        result.message = "휴가등록이 완료되었습니다.";
        try {
            result.data = tbPb204Repository.save(tbPb204);
        }catch (Exception e){
            e.printStackTrace();
            result.message = "오류가 발생하였습니다.";
        }
        return result;
    }
    // 휴가구분 선택(근태설정에서 설정값있다면 적용)
    @GetMapping("/bindPeriod")
    public AjaxResult bindPeriod (@RequestParam Map<String, String> params,
                                   HttpServletRequest request) {
        AjaxResult result = new AjaxResult();
        String attKind = params.get("attKind");
        Map<String, Object> attInfo = attendanceSubmitService.getPeriod(attKind);

        result.data = attInfo;
        return result;
    }
    // 날자 시간 분리 메서드
    private Map<String, String> extractDateTimeParts(String dateTime) {
        Map<String, String> dateTimeParts = new HashMap<>();

        if (dateTime != null && dateTime.contains("T")) {
            String[] parts = dateTime.split("T");
            dateTimeParts.put("date", parts[0].replaceAll("-", "")); // YYYYMMDD
            dateTimeParts.put("time", parts[1]);                     // HH:mm
        } else {
            dateTimeParts.put("date", null);
            dateTimeParts.put("time", null);
        }

        return dateTimeParts;
    }

}
