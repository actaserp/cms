package mes.app.clock;

import mes.app.clock.service.ClockMemberService;
import mes.domain.entity.Tb_pb203;
import mes.domain.entity.Tb_pb203Id;
import mes.domain.entity.User;
import mes.domain.entity.commute.TB_PB201;
import mes.domain.entity.commute.TB_PB201_PK;
import mes.domain.entity.mobile.TB_PB204;
import mes.domain.model.AjaxResult;
import mes.domain.repository.commute.TB_PB201Repository;
import mes.domain.repository.mobile.TB_PB204Repository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api/clock/member")
public class ClockMemberController {

    @Autowired
    private ClockMemberService clockMemberService;

    @Autowired
    TB_PB204Repository tbPb204Repository;

    @Autowired
    TB_PB201Repository tbPb201Repository;

    @GetMapping("/read")
    public AjaxResult getMemberList(
            @RequestParam(value="start_date", required=false) String start_date,
            @RequestParam(value="end_date", required=false) String end_date,
            @RequestParam(value="person_name", required=false) String person_name,
            @RequestParam(value ="spjangcd") String spjangcd,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();

        if (start_date != null && start_date.contains("-")) {
            start_date = start_date.replaceAll("-", "");
        }
        if (end_date != null && end_date.contains("-")) {
            end_date = end_date.replaceAll("-", "");
        }

        List<Map<String, Object>> items = this.clockMemberService.getMemberList(start_date,end_date,person_name,spjangcd);
        result.data = items;
        return result;
    }

    @PostMapping("/save")
    @Transactional
    public AjaxResult saveMemberList(
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

        List<TB_PB204> tbpb204List = new ArrayList<>();

        for (Map<String, Object> item : dataList) {
            Integer id = ((Number) item.get("id")).intValue();

            Optional<TB_PB204> optional = tbPb204Repository.findById(id);

            if (optional.isPresent()) {
                TB_PB204 tbpb204 = optional.get();
                tbpb204.setFixflag("1");
                tbpb204List.add(tbpb204);

                // 1. 날짜 범위 파싱
                String frdateStr = tbpb204.getFrdate(); // "yyyyMMdd"
                String todateStr = tbpb204.getTodate();
                if(frdateStr == null || todateStr == null) continue;

                LocalDate frdate = LocalDate.parse(frdateStr, DateTimeFormatter.ofPattern("yyyyMMdd"));
                LocalDate todate = LocalDate.parse(todateStr, DateTimeFormatter.ofPattern("yyyyMMdd"));

                for (LocalDate date = frdate; !date.isAfter(todate); date = date.plusDays(1)) {
                    String workym = date.format(DateTimeFormatter.ofPattern("yyyyMM"));
                    String workday = date.format(DateTimeFormatter.ofPattern("dd"));

                    TB_PB201_PK pk = new TB_PB201_PK();
                    pk.setSpjangcd(spjangcd);
                    pk.setWorkym(workym);
                    pk.setWorkday(workday);
                    pk.setPersonid(tbpb204.getPersonid());

                    // 이미 있으면 update, 없으면 insert
                    Optional<TB_PB201> existed = tbPb201Repository.findById(pk);
                    TB_PB201 entity;
                    if(existed.isPresent()) {
                        entity = existed.get();
                    } else {
                        entity = new TB_PB201();
                        entity.setId(pk);
                    }
                    // 연차 근태값 세팅
                    entity.setWorkcd(tbpb204.getWorkcd()); // 연차 코드
                    entity.setRemark("연차 자동반영"); // 필요시

                    tbPb201Repository.save(entity);
                }
            }
        }

        // 저장
        List<TB_PB204> savedList = tbPb204Repository.saveAll(tbpb204List);

        result.success = true;
        result.data = savedList;
        return result;
    }

    @PostMapping("/Cancel")
    @Transactional
    public AjaxResult CancelMemberList(
            @RequestBody Map<String, Object> requestData,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();
        List<Map<String, Object>> dataList = (List<Map<String, Object>>) requestData.get("list");

        for (Map<String, Object> item : dataList) {
            String spjangcd = (String) item.get("spjangcd"); // 사업장코드
            Integer id = ((Number) item.get("id")).intValue(); // 사번

            Optional<TB_PB204> optional = tbPb204Repository.findById(id);
            if (optional.isPresent()) {
                TB_PB204 entity = optional.get();
                entity.setFixflag("0"); // fixflag를 "0"으로 설정
                tbPb204Repository.save(entity); // 변경사항 저장
            }
        }

        result.success = true;
        return result;
    }



}
