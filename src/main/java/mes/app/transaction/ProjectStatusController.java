package mes.app.transaction;

import lombok.extern.slf4j.Slf4j;
import mes.app.transaction.service.ProjectStatusService;
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
@RequestMapping("/api/transaction/ProjectStatus")
public class ProjectStatusController {

  @Autowired
  ProjectStatusService projectStatusService;

  @GetMapping("/allRead")
  public AjaxResult getProjectStatusList(@RequestParam(value ="spjangcd") String spjangcd,
                                         @RequestParam(value = "cboYear") String cboYear,
                                         @RequestParam(value = "txtProjectName", required = false) String txtProjectName,
                                         HttpServletRequest request) {

    List<Map<String, Object>> items = this.projectStatusService.getProjectStatusList( spjangcd, txtProjectName, cboYear);

    AjaxResult result = new AjaxResult();
    result.data = items;

    return result;
  }

  //경비 사용내역
  @GetMapping("/ExpenseHistory")
  public AjaxResult getExpenseHistory(@RequestParam(value ="spjangcd") String spjangcd,
                                         @RequestParam(value = "projno", required = false) String projno,
                                         HttpServletRequest request) {

    List<Map<String, Object>> items = this.projectStatusService.getExpenseHistory( spjangcd, projno);

    AjaxResult result = new AjaxResult();
    result.data = items;

    return result;
  }

  //매출내역
  @GetMapping("/SalesHistory")
  public AjaxResult getSalesHistory(@RequestParam(value ="spjangcd") String spjangcd,
                                      @RequestParam(value = "projno") String projno,
                                      HttpServletRequest request) {

    List<Map<String, Object>> items = this.projectStatusService.getSalesHistory( spjangcd, projno);

    AjaxResult result = new AjaxResult();
    result.data = items;

    return result;
  }

  //입출금내역
  @GetMapping("/TransactionHistory")
  public AjaxResult getTransactionHistory(@RequestParam(value ="spjangcd") String spjangcd,
                                      @RequestParam(value = "projno") String projno,
                                      HttpServletRequest request) {

    List<Map<String, Object>> items = this.projectStatusService.getTransactionHistory( spjangcd, projno );

    AjaxResult result = new AjaxResult();
    result.data = items;

    return result;
  }

}
