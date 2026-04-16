package mes.app.summary;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.summary.service.SujuMonthSummarySerivce;
import mes.domain.model.AjaxResult;

@RestController
@RequestMapping("/api/summary/suju_month_summary")
public class SujuMonthSummaryController {
	
	@Autowired
	SujuMonthSummarySerivce sujuMonthSummarySerivce;
	
	
	@GetMapping("/read")
	public AjaxResult getSujuMonthList(
			@RequestParam(value="cboYear",required=false) String cboYear,
			@RequestParam(value="cboCompany",required=false) Integer cboCompany,
			@RequestParam(value="cboMatGrp",required=false) Integer cboMatGrp,
			@RequestParam(value="cboDataDiv",required=false) String cboDataDiv,
			@RequestParam(value="spjangcd",required=false) String spjangcd
			) {
		
		List<Map<String,Object>> items = this.sujuMonthSummarySerivce.getList(cboYear,cboCompany,cboMatGrp,cboDataDiv,spjangcd);
		
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result; 
	}

}
