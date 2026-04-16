package mes.app.summary;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.summary.service.ProductionDefectTypeMonthService;
import mes.domain.model.AjaxResult;

@RestController
@RequestMapping("/api/summary/production_defect_type_month_portion")
public class ProductionDefectTypeMonthController {
	
	@Autowired
	ProductionDefectTypeMonthService productionDefectTypeMonthService;
	
	
	@GetMapping("/read")
	public AjaxResult getProductionDefectTypeMonthList(
			@RequestParam(value="cboYear",required=false) String cboYear,
			@RequestParam(value="cboMatType",required=false) Integer cboMatType,
			@RequestParam(value="cboMatGrpPk",required=false) Integer cboMatGrpPk,
			@RequestParam(value="txtProductId",required=false) Integer txtProductId
			) {
		
		List<Map<String,Object>> items = this.productionDefectTypeMonthService.getList(cboYear,cboMatType,cboMatGrpPk,txtProductId);
		
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result; 
	}
	@GetMapping("/readProduct")
	public AjaxResult getProductionDetailDefectTypeMonthList(
			@RequestParam(value="cboYear",required=false) String cboYear,
			@RequestParam(value="cboMatType",required=false) String cboMatType,
			@RequestParam(value="cboMatGrpPk",required=false) Integer cboMatGrpPk,
			@RequestParam(value="txtProductId",required=false) Integer txtProductId
	) {

		List<Map<String,Object>> items = this.productionDefectTypeMonthService.getProductList(cboYear,cboMatType,cboMatGrpPk,txtProductId);


		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}
}
