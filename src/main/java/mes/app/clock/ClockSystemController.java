package mes.app.clock;

import mes.app.clock.service.ClockSystemService;
import mes.domain.entity.*;
import mes.domain.model.AjaxResult;
import mes.domain.repository.Tb_pb210Repository;
import mes.domain.repository.Tb_pbcontRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/clock/System")
public class ClockSystemController {

    @Autowired
    private ClockSystemService clockSystemService;

    @Autowired
    private Tb_pb210Repository tb_pb210Repository;

    @Autowired
    private Tb_pbcontRepository tb_pbcontRepository;

    @GetMapping("/read")
    public AjaxResult getSystemList(
            @RequestParam(value ="spjangcd") String spjangcd,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();

        List<Map<String, Object>> items = this.clockSystemService.getSystemList(spjangcd);
        result.data = items;
        return result;
    }


    @GetMapping("/tiemread")
    public AjaxResult getSystemtimeList(
            @RequestParam(value ="spjangcd") String spjangcd,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();

        List<Map<String, Object>> items = this.clockSystemService.getSystemtimeList(spjangcd);
        result.data = items;
        return result;
    }


    @GetMapping("/detail")
    public AjaxResult getSystemDetail(
            @RequestParam(value="workcd") String workcd,
            @RequestParam(value ="spjangcd") String spjangcd,
            HttpServletRequest request) {

        Map<String, Object> item = this.clockSystemService.getSystemDetail(workcd,spjangcd);
        AjaxResult result = new AjaxResult();
        result.data = item;
        return result;
    }


    @PostMapping("/save")
    public AjaxResult saveBom(
            @RequestParam(value = "workcd") String workcd,
            @RequestParam(value = "worknm", required = false) String worknm,
            @RequestParam(value = "remark", required = false) String remark,
            @RequestParam(value = "yearflag", required = false) String yearflag,
            @RequestParam(value = "usenum", required = false) String usenum,
            @RequestParam(value = "spjangcd") String spjangcd,
            Authentication auth
    ) {

        User user = (User) auth.getPrincipal();

        AjaxResult result = new AjaxResult();

        // ID 객체 생성 및 설정
        Tb_pb210Id id = new Tb_pb210Id(spjangcd, workcd);

        // 엔티티 객체 생성 및 필드 설정
        Tb_pb210 tb_pb210 = new Tb_pb210();
        tb_pb210.setId(id);
        tb_pb210.setWorknm(worknm);
        tb_pb210.setRemark(remark);
        tb_pb210.setYearflag(yearflag);
        if (usenum != null && !usenum.isEmpty()) {
            tb_pb210.setUsenum(new BigDecimal(usenum));
        }

        // 저장
        tb_pb210 = this.tb_pb210Repository.save(tb_pb210);

        result.data = tb_pb210;

        return result;
    }

    @PostMapping("/delete")
    public AjaxResult deleteSystem(@RequestParam("workcd") String workcd,
                                 @RequestParam("spjangcd") String spjangcd) {
        Tb_pb210Id id = new Tb_pb210Id(spjangcd, workcd); // 복합키 객체 생성
        tb_pb210Repository.deleteById(id);                // 복합키로 삭제

        AjaxResult result = new AjaxResult();
        return result;
    }




    @PostMapping("/savetime")
    @Transactional
    public AjaxResult saveTime(@RequestBody Map<String, Object> body,
                               HttpServletRequest request,
                               Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();

        // 1. spjangcd 분리
        String spjangcd = (String) body.get("spjangcd");

        // 2. data 리스트 추출
        List<Map<String, Object>> dataList = (List<Map<String, Object>>) body.get("data");
        List<Tb_pbcont> Tb_pbcontList = new ArrayList<>();

        for (Map<String, Object> item : dataList) {
            // 3. 복합키 생성
            String flag = (String) item.get("flag");
            Tb_pbcontId id = new Tb_pbcontId(spjangcd, flag);

            // 4. 엔티티 생성 및 값 세팅
            Tb_pbcont entity = new Tb_pbcont();
            entity.setId(id);
            entity.setSttime((String) item.get("sttime"));
            entity.setEndtime((String) item.get("endtime"));
            entity.setOvsttime((String) item.get("ovsttime"));
            entity.setOvedtime((String) item.get("ovedtime"));
            entity.setNgsttime((String) item.get("ngsttime"));
            entity.setNgedtime((String) item.get("ngedtime"));

            Tb_pbcontList.add(entity);
        }

        // 5. 저장
        List<Tb_pbcont> savedList = tb_pbcontRepository.saveAll(Tb_pbcontList);

        result.success = true;
        result.data = savedList;
        return result;
    }



}
