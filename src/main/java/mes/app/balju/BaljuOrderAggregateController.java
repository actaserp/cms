package mes.app.balju;

import lombok.extern.slf4j.Slf4j;
import mes.app.balju.service.BaljuOrderAggregateService;
import mes.domain.model.AjaxResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/balju/balju_order_aggregate")
public class BaljuOrderAggregateController {

  @Autowired
  BaljuOrderAggregateService baljuOrderAggregateService;

  @GetMapping("/read")
  public AjaxResult getBaljuMonthList(
      @RequestParam(value="srchStartDt",required=false) String srchStartDt,
      @RequestParam(value="srchEndDt",required=false) String srchEndDt,
      @RequestParam(value="cboCompany",required=false) Integer cboCompany,
      @RequestParam(value="cboMatGrp",required=false) Integer cboMatGrp,
      @RequestParam(value = "cBaljuState", required = false) String cBaljuState,
      @RequestParam(value = "spjangcd") String spjangcd
  ) {

    List<Map<String,Object>> items = this.baljuOrderAggregateService.getList(srchStartDt,srchEndDt,cboCompany,cboMatGrp,cBaljuState, spjangcd);


    AjaxResult result = new AjaxResult();
    result.data = items;
    return result;
  }
}
