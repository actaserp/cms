package mes.app.summary;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.summary.service.ProductionMonthDefectProService;
import mes.domain.model.AjaxResult;

@RestController
@RequestMapping("/api/summary/production_month_defect_pro")
public class ProductionMonthDefectProController {
	
	@Autowired
	ProductionMonthDefectProService productionMonthDefectProService;
	
	
	@GetMapping("/read")
	public AjaxResult getProductionMonthProList(
			@RequestParam(value="cboYear",required=false) String cboYear,
			@RequestParam(value="cboMatType",required=false) String cboMatType,
			@RequestParam(value="cboMatGrpPk",required=false) Integer cboMatGrpPk,
			@RequestParam(value="cboDataDiv",required=false) String cboDataDiv,
			@RequestParam(value="chkOnlyDefect",required=false) String chkOnlyDefect
			) {
		
		List<Map<String,Object>> items = this.productionMonthDefectProService.getList(cboYear,cboMatType,cboMatGrpPk,cboDataDiv,chkOnlyDefect);
		
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result; 
	}

}
