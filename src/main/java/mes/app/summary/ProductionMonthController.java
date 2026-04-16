package mes.app.summary;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.summary.service.ProductionMonthService;
import mes.domain.model.AjaxResult;

@RestController
@RequestMapping("/api/summary/production_month_summary")
public class ProductionMonthController {
	
	@Autowired
	ProductionMonthService productionMonthService;
	
	@GetMapping("/read")
	public AjaxResult getProductionMonthList(
			@RequestParam(value="cboYear",required=false) String cboYear,
			@RequestParam(value="cbomatType",required=false) String cbomatType,
			@RequestParam(value="matGrpPk",required=false) Integer matGrpPk,
			@RequestParam(value="cboDataDiv",required=false) String cboDataDiv) {
		
		
		List<Map<String,Object>> items = this.productionMonthService.getList(cboYear,cbomatType,matGrpPk,cboDataDiv);
		
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
		
		
	}
}
