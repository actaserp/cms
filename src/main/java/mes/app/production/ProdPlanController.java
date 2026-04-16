package mes.app.production;

import lombok.extern.slf4j.Slf4j;
import mes.app.production.service.ProdPlanServicr;
import mes.domain.entity.Suju;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.JobResRepository;
import mes.domain.repository.MaterialRepository;
import mes.domain.repository.RoutingProcRepository;
import mes.domain.repository.SujuRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import javax.transaction.Transactional;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/production/prod_plan")
public class ProdPlanController {


  @Autowired
  private ProdPlanServicr prodPlanServicr;

  @Autowired
  MaterialRepository materialRepository;

  @Autowired
  RoutingProcRepository routingProcRepository;

  @Autowired
  JobResRepository jobResRepository;

  @Autowired
  SujuRepository sujuRepository;

  // 수주 목록 조회
  @GetMapping("/suju_list")
  public AjaxResult getSujuList(
      @RequestParam(value="date_kind", required=false) String date_kind,
      @RequestParam(value="start", required=false) String start,
      @RequestParam(value="end", required=false) String end,
      @RequestParam(value="mat_group", required=false) Integer mat_group,
      @RequestParam(value="mat_name", required=false) String mat_name,
      @RequestParam("spjangcd") String spjangcd,
      @RequestParam(value="not_flag", required=false) String not_flag) {
    /*log.info("작업계회 등록 목록: date_kind:{}, start:{}, end:{}, mat_group:{},mat_name:{}, spjangcd:{}, not_flag:{}",
        date_kind, start, end, mat_group, mat_name, spjangcd, not_flag);*/
    List<Map<String, Object>> items = this.prodPlanServicr.getSujuList(date_kind, start, end, mat_group, mat_name, not_flag, spjangcd);

    AjaxResult result = new AjaxResult();
    result.data = items;

    return result;
  }

  //수주확정
  @PostMapping("/plane_confirm")
  @Transactional
  public AjaxResult SujuConfirm(@RequestParam(value = "suju_id", required = false) Integer sujuId,
                                Authentication auth) {
    AjaxResult result = new AjaxResult();
    User user = (User) auth.getPrincipal();

    if (sujuId != null) {
      Suju suju = sujuRepository.getSujuById(sujuId);  // 수주 엔티티 조회
      if (suju != null) {
        suju.setConfirm("1");             // 확정 처리
        suju.setState("planned");
        sujuRepository.save(suju);      // 저장

        result.success = true;
        result.message = "확정되었습니다.";
      } else {
        result.success = false;
        result.message = "수주 정보를 찾을 수 없습니다.";
      }
    } else {
      result.success = false;
      result.message = "수주 ID가 없습니다.";
    }

    return result;
  }

}
