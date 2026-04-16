package mes.app.transaction;

import lombok.extern.slf4j.Slf4j;
import mes.app.transaction.service.ExpenseAccountSetupService;
import mes.domain.entity.TB_CA648;
import mes.domain.entity.TB_CA648Id;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.TB_ca648Repository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@RestController
@RequestMapping("/api/transaction/ExpenseAccountSetup")
public class ExpenseAccountSetupController {

  @Autowired
  ExpenseAccountSetupService accountSetupService;

  @Autowired
  TB_ca648Repository tb_ca648Repository;

  @GetMapping("/read")
  public AjaxResult getExpenseAccountList(@RequestParam (value = "txtDescription") String txtDescription,
                                          @RequestParam(value ="spjangcd") String spjangcd) {
//    log.info("비용항목 등록 read - spjangcd:{}, txtDescription::{}",spjangcd, txtDescription);

    List<Map<String, Object>> items = this.accountSetupService.getExpenseAccountList(spjangcd, txtDescription);

    AjaxResult result = new AjaxResult();
    result.data = items;

    return result;
  }

  @GetMapping("/readDetail")
  public AjaxResult getExpenseAccountDetail(@RequestParam(value ="groupCode") String groupCode,
                                            @RequestParam(value ="spjangcd") String spjangcd) {
//    log.info("비용항목 상세 - groupCode:{}",groupCode);

    List<Map<String, Object>> items = this.accountSetupService.getExpenseAccountDetail(groupCode,spjangcd);

    AjaxResult result = new AjaxResult();
    result.data = items;

    return result;
  }

  //저장
  @PostMapping("/save")
  public AjaxResult saveExpenseItems(@RequestBody Map<String, Object> payload, Authentication auth) {
    AjaxResult result = new AjaxResult();

    // 사용자 정보
    User user = (User) auth.getPrincipal();
    String username = user.getUserProfile().getName();
    String today = LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE); // "yyyyMMdd"

    // 기본 값들
    String spjangcd = (String) payload.get("spjangcd");
    String gartcdRoot = (String) payload.get("gartcd");
    String gartName = (String) payload.get("gart_name");
    String remark = (String) payload.get("remark");

    // 그룹코드 remark 저장 처리 추가 (sys_code 테이블)
    accountSetupService.saveGroupRemark(spjangcd, gartcdRoot, gartName, remark);

    // 상세 항목 저장 처리
    List<Map<String, Object>> details = (List<Map<String, Object>>) payload.get("details");

    for (Map<String, Object> row : details) {
      // row에 gartcd가 없다면 상위에서 보완
      String gartcd = row.get("gartcd") != null ? (String) row.get("gartcd") : gartcdRoot;
      String artcd = (String) row.get("artcd");

      boolean isNew = (artcd == null || artcd.trim().isEmpty());

      if (isNew) {
        artcd = generateArtcd(spjangcd, gartcd);
        row.put("artcd", artcd); // 클라이언트에 다시 보내려면 필요
      }

      TB_CA648Id id = new TB_CA648Id(spjangcd, gartcd, artcd);
      Optional<TB_CA648> optional = tb_ca648Repository.findById(id);

      // Boolean useyn 처리
      Object useynObj = row.get("useyn");
      String useyn = (useynObj instanceof Boolean)
          ? ((Boolean) useynObj ? "1" : "0")
          : (String) useynObj;

      if (optional.isPresent() && !isNew) {
        // 수정
        TB_CA648 existing = optional.get();
        existing.setArtnm((String) row.get("artnm"));
        existing.setJiflag((String) row.get("jiflag"));
        existing.setUseyn(useyn);
        existing.setGflag((String) row.get("gflag"));
        existing.setAcccd((String) row.get("acccd"));
        existing.setAccnm((String) row.get("accnm"));
        existing.setWacccd((String) row.get("wacccd"));
        existing.setWaccnm((String) row.get("waccnm"));
        existing.setSacccd((String) row.get("sacccd"));
        existing.setSaccnm((String) row.get("saccnm"));
        existing.setIndate(today);
        existing.setInuserid(username);

        tb_ca648Repository.save(existing);
      } else {
        // 신규 등록
        TB_CA648 newItem = new TB_CA648();
        newItem.setId(id);
        newItem.setArtnm((String) row.get("artnm"));
        newItem.setJiflag((String) row.get("jiflag"));
        newItem.setUseyn(useyn);
        newItem.setGflag((String) row.get("gflag"));
        newItem.setAcccd((String) row.get("acccd"));
        newItem.setAccnm((String) row.get("accnm"));
        newItem.setWacccd((String) row.get("wacccd"));
        newItem.setWaccnm((String) row.get("waccnm"));
        newItem.setSacccd((String) row.get("sacccd"));
        newItem.setSaccnm((String) row.get("saccnm"));
        newItem.setIndate(today);
        newItem.setInuserid(username);

        tb_ca648Repository.save(newItem);
        //log.info("신규 등록됨: {}", id);
      }
    }

    result.success = true;
    result.message = "저장 완료";
    return result;
  }

  private String generateArtcd(String spjangcd, String gartcd) {
    String maxSuffix = accountSetupService.findgartcd(spjangcd, gartcd); // ex: "03"
    int nextNum = (maxSuffix != null && !maxSuffix.isEmpty()) ? Integer.parseInt(maxSuffix) + 1 : 1;
    String padded = String.format("%02d", nextNum); // 항상 두 자리
    return gartcd + padded; // "05" + "04" => "0504"
  }

  //행삭제
  @PostMapping("/delete")
  public @ResponseBody AjaxResult deleteData(@RequestParam("artcd") String artcd,
                                             @RequestParam("gartcd") String gartcd,
                                             @RequestParam("spjangcd") String spjangcd) {
    //log.info("행 삭제 요청 들어옴 --- artcd:{}, gartcd:{}, spjangcd :{}",artcd,gartcd, spjangcd);
    AjaxResult result = new AjaxResult();
    TB_CA648Id id = new TB_CA648Id(spjangcd, gartcd, artcd);

    if (tb_ca648Repository.existsById(id)) {
      tb_ca648Repository.deleteById(id);
      result.success = true;
      result.message = "삭제 완료";
    } else {
      result.success = (false);
      result.message = ("해당 데이터 없음");
    }

    return result;
  }

  @PostMapping("/deleteGroup")
  public @ResponseBody AjaxResult deleteGroup(@RequestBody Map<String, Object> param) {
    String gartcd = (String) param.get("gartcd");
    String spjangcd = (String) param.get("spjangcd");

    //log.info("그룹 삭제 요청: gartcd={}, spjangcd={}", gartcd, spjangcd);

    AjaxResult result = new AjaxResult();

    try {
      accountSetupService.deleteGroupAndItems(gartcd, spjangcd);
      result.success = true;
      result.message = "그룹 및 상세항목 삭제 완료";
    } catch (Exception e) {
      log.error("삭제 중 오류 발생", e);
      result.success = false;
      result.message = "삭제 실패: " + e.getMessage();
    }

    return result;
  }
}
