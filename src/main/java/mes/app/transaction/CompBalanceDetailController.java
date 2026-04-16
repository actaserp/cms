package mes.app.transaction;

import lombok.extern.slf4j.Slf4j;
import mes.app.aspect.DecryptField;
import mes.app.transaction.service.CompBalanceDetailServicr;
import mes.domain.model.AjaxResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/transaction/CompBalanceDetail")
public class CompBalanceDetailController {

  @Autowired
  CompBalanceDetailServicr compBalanceDetailServicr;

  // 거래처잔액 명세(입금 관리)
  @DecryptField(columns  = {"accnum"})
  @GetMapping("/read")
  public AjaxResult getList(
      @RequestParam(value="srchStartDt", required=false) String start,
      @RequestParam(value="srchEndDt", required=false) String end,
      @RequestParam(value = "cboCompany", required=false) String company,
      @RequestParam(value = "spjangcd") String spjangcd,
      HttpServletRequest request) {
    //log.info("거래처잔액 명세(입금 관리) read ---  :start:{}, end:{} ,company:{}, spjangcd:{} ", start_date, end_date, company, spjangcd);

    List<Map<String, Object>> items = this.compBalanceDetailServicr.getList(start, end, company,spjangcd);

    AjaxResult result = new AjaxResult();
    result.data = items;

    return result;
  }
}
