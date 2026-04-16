package mes.app.summary;

import mes.app.summary.service.ProductionDefectService;
import mes.app.summary.service.ProductionDefectTypeMonthService;
import mes.domain.model.AjaxResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/summary/production_defect_portion")
public class ProductionDefectController {
	
	@Autowired
	ProductionDefectService productionDefectService;
	
	
	@GetMapping("/read")
	public AjaxResult getProductionDefectTypeMonthList(
			@RequestParam(value="date_from",required=false) String date_from,
			@RequestParam(value="date_to",required=false) String date_to,
			@RequestParam(value="cboWorkCenter",required=false) Integer cboWorkCenter
			) {
		
		List<Map<String,Object>> items = this.productionDefectService.getList(date_from, date_to, cboWorkCenter);
		
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result; 
	}

}
