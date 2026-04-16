package mes.app.balju;

import lombok.extern.slf4j.Slf4j;
import mes.app.balju.service.BaljuOrderListService;
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
@RequestMapping("/api/balju/balju_order_list")
public class BaljuOrderListController {

  @Autowired
  BaljuOrderListService baljuOrderListService;

  @GetMapping("/read")
  public AjaxResult getSujuMonthList(
      @RequestParam(value="cboYear",required=false) String cboYear,
      @RequestParam(value="cboCompany",required=false) Integer cboCompany,
      @RequestParam(value="cboMatGrp",required=false) Integer cboMatGrp,
      @RequestParam(value="cboDataDiv",required=false) String cboDataDiv,
      @RequestParam(value = "spjangcd") String spjangcd
  ) {
//    log.info("월별 발주량 read : cboYear:{}, cboCompany:{}, cboMatGrp:{}, cboDataDiv:{}, spjangcd:{} ",
//    cboYear, cboCompany, cboMatGrp, cboDataDiv, spjangcd);
    List<Map<String,Object>> items = this.baljuOrderListService.getList(cboYear,cboCompany,cboMatGrp,cboDataDiv, spjangcd);

    AjaxResult result = new AjaxResult();
    result.data = items;
    return result;
  }

}
