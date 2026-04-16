package mes.app.clock;

import lombok.extern.slf4j.Slf4j;
import mes.app.clock.service.ClockYearlyService;
import mes.domain.entity.Tb_pb203;
import mes.domain.entity.Tb_pb209;
import mes.domain.entity.User;
import mes.domain.entity.commute.TB_PB201;
import mes.domain.model.AjaxResult;
import mes.domain.repository.Tb_pb209Repository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@RestController
@RequestMapping("/api/clock/Yearly")
public class YearlyController {

    @Autowired
    private ClockYearlyService clockYearlyService;

    @Autowired
    private Tb_pb209Repository tpb209Repository;

    @GetMapping("/read")
    public AjaxResult getYearlyList(
            @RequestParam(value="year") String year,
            @RequestParam(value="name",required=false) String name,
            @RequestParam(value ="spjangcd") String spjangcd,
            @RequestParam(value="startdate",required=false) String startdate,
            @RequestParam(value="rtflag",required=false) String rtflag,
            HttpServletRequest request) {

//        log.info("연차관리 들어옴 데이터 ---  year:{}, name:{}, startdate2:{}, rtflag:{}", year, name, startdate, rtflag);
        if (startdate != null && startdate.contains("-")) {
            startdate = startdate.replaceAll("-", "");
        }


        List<Map<String, Object>> items = this.clockYearlyService.getYearlyList(year,name,spjangcd,startdate,rtflag);
        for(Map<String, Object> item : items){
            Object rtdateObj = item.get("rtdate");
            if (rtdateObj != null && rtdateObj.toString().length() == 8) {
                String rtdate = rtdateObj.toString();
                // yyyy-mm-dd 변환
                String formatted = rtdate.substring(0, 4) + "-" + rtdate.substring(4, 6) + "-" + rtdate.substring(6, 8);
                item.put("rtdate", formatted);
            }
        }
        AjaxResult result = new AjaxResult();
        result.data = items;

        return result;
    }

    @GetMapping("/YearlyCreat")
    public AjaxResult getYearlyCreat(
            @RequestParam(value="year") String year,
            @RequestParam(value ="spjangcd") String spjangcd,
            @RequestParam(value="startdate",required=false) String startdate,
            @RequestParam(value="name",required=false) String name,
            HttpServletRequest request) {


        if (startdate != null && startdate.contains("-")) {
            startdate = startdate.replaceAll("-", "");
        }


        List<Map<String, Object>> items = this.clockYearlyService.YearlyCreate(year,spjangcd,startdate,name);

        AjaxResult result = new AjaxResult();
        result.data = items;

        return result;
    }

    @GetMapping("/MonthlyCreate")
    public AjaxResult getMonthlyCreate(
            @RequestParam(value="year") String year,
            @RequestParam(value ="spjangcd") String spjangcd,
            @RequestParam(value="startdate",required=false) String startdate,
            @RequestParam(value="name",required=false) String name,
            HttpServletRequest request) {


        if (startdate != null && startdate.contains("-")) {
            startdate = startdate.replaceAll("-", "");
        }


        List<Map<String, Object>> items = this.clockYearlyService.MonthlyCreate(year,spjangcd,startdate,name);

        AjaxResult result = new AjaxResult();
        result.data = items;

        return result;
    }


    @PostMapping("/Yearlysave")
    @Transactional
    public AjaxResult saveYearlysave(
            @RequestBody Map<String, Object> requestData,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();
        User user = (User)auth.getPrincipal();

        List<Map<String, Object>> dataList = (List<Map<String, Object>>) requestData.get("saveList");
        String spjangcd = (String) requestData.get("spjangcd");
        String yearStr = (String) requestData.get("year"); // 추가
        int reqDate = Integer.parseInt(yearStr + "0000");  // 추가


        List<Tb_pb209> tbpb209List = new ArrayList<>();

        for (Map<String, Object> item : dataList) {
            Object ewolnumStr = item.get("ewolnum");  //이월일수
            Object holinumStr = item.get("holinum"); // 연차생성일수
            Integer id = ((Number) item.get("id")).intValue(); //id
            Object restnumStr = item.get("restnum"); //잔여일수

            Optional<Tb_pb209> optional = tpb209Repository.findByPersonidAndReqdate(id, String.valueOf(reqDate));

            Tb_pb209 tbpb209;

            if (optional.isPresent()) {
                // 기존 데이터 수정
                tbpb209 = optional.get();
            }else{
                // 새로운 데이터 추가
                tbpb209 = new Tb_pb209();
                tbpb209.setPersonid(id);
                tbpb209.setReqdate(String.valueOf(reqDate));
                tbpb209.setSpjangcd(spjangcd);
                tbpb209.setHflag("0");
            }


            tbpb209.setEwolnum(new BigDecimal(ewolnumStr.toString()));
            tbpb209.setHolinum(new BigDecimal(holinumStr.toString()));
            tbpb209.setRestnum(new BigDecimal(restnumStr.toString()));

            tbpb209List.add(tbpb209);

        }
        // 저장
        tpb209Repository.saveAll(tbpb209List);

        result.success = true;
        result.data = tbpb209List;
        return result;
    }

    @PostMapping("/Monthlysave")
    @Transactional
    public AjaxResult saveMonthlysave(
            @RequestBody Map<String, Object> requestData,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();
        User user = (User)auth.getPrincipal();

        List<Map<String, Object>> dataList = (List<Map<String, Object>>) requestData.get("saveList");
        String spjangcd = (String) requestData.get("spjangcd");
        String yearStr = (String) requestData.get("year"); // 추가
        int reqDate = Integer.parseInt(yearStr + "0000");  // 추가


        List<Tb_pb209> tbpb209List = new ArrayList<>();

        for (Map<String, Object> item : dataList) {
            Object ewolnumStr = item.get("ewolnum");  //이월일수
            Object holinumStr = item.get("holinum"); // 연차생성일수
            Integer id = ((Number) item.get("id")).intValue(); //id
            Object restnumStr = item.get("restnum"); //잔여일수

            Optional<Tb_pb209> optional = tpb209Repository.findByPersonidAndReqdate(id, String.valueOf(reqDate));

            Tb_pb209 tbpb209;

            if (optional.isPresent()) {
                // 기존 데이터 수정
                tbpb209 = optional.get();
            }else{
                // 새로운 데이터 추가
                tbpb209 = new Tb_pb209();
                tbpb209.setPersonid(id);
                tbpb209.setReqdate(String.valueOf(reqDate));
                tbpb209.setSpjangcd(spjangcd);
                tbpb209.setHflag("0");
            }


            tbpb209.setEwolnum(toBigDecimalOrNull(ewolnumStr));
            tbpb209.setHolinum(toBigDecimalOrNull(holinumStr));
            tbpb209.setRestnum(toBigDecimalOrNull(restnumStr));


            tbpb209List.add(tbpb209);

        }
        // 저장
        tpb209Repository.saveAll(tbpb209List);

        result.success = true;
        result.data = tbpb209List;
        return result;
    }

    private BigDecimal toBigDecimalOrNull(Object value) {
        return value != null ? new BigDecimal(value.toString()) : null;
    }


    @GetMapping("/detail")
    public AjaxResult getYearlyDetail(
            @RequestParam(value="id") Integer id,
            @RequestParam(value="year") String year,
            HttpServletRequest request) {

        AjaxResult result = new AjaxResult();

        List<Map<String, Object>> item = this.clockYearlyService.getYearlyDetail(id,year);
        for(Map<String, Object> detail : item){
            Object reqdateObj = detail.get("reqdate");
            Object frdateObj = detail.get("frdate");
            Object todateObj = detail.get("todate");
            if (reqdateObj != null && reqdateObj.toString().length() == 8) {
                String reqdate = reqdateObj.toString();
                // yyyy-mm-dd 변환
                String formatted = reqdate.substring(0, 4) + "-" + reqdate.substring(4, 6) + "-" + reqdate.substring(6, 8);
                detail.put("reqdate", formatted);
            }
            if (frdateObj != null && frdateObj.toString().length() == 8) {
                String frdate = frdateObj.toString();
                // yyyy-mm-dd 변환
                String formatted = frdate.substring(0, 4) + "-" + frdate.substring(4, 6) + "-" + frdate.substring(6, 8);
                detail.put("frdate", formatted);
            }
            if (todateObj != null && todateObj.toString().length() == 8) {
                String todate = todateObj.toString();
                // yyyy-mm-dd 변환
                String formatted = todate.substring(0, 4) + "-" + todate.substring(4, 6) + "-" + todate.substring(6, 8);
                detail.put("todate", formatted);
            }
        }
        result.data = item;
        return result;
    }

}
