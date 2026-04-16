package mes.app.summary;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.summary.service.SujuSummaryService;
import mes.domain.model.AjaxResult;

@RestController
@RequestMapping("/api/summary/suju_summary")
public class SujuSummaryController {
	
	@Autowired
	SujuSummaryService sujuSummaryService;
	
	
	@GetMapping("/read")
	public AjaxResult getSujuMonthList(
			@RequestParam(value="srchStartDt",required=false) String srchStartDt,
			@RequestParam(value="srchEndDt",required=false) String srchEndDt,
			@RequestParam(value="cboCompany",required=false) Integer cboCompany,
			@RequestParam(value="cboMatGrp",required=false) Integer cboMatGrp,
			@RequestParam(value="sujuState",required=false) String sujuState,
			@RequestParam(value="spjangcd",required=false) String spjangcd
			) {
		
		List<Map<String,Object>> items = this.sujuSummaryService.getList(srchStartDt,srchEndDt,cboCompany,cboMatGrp,sujuState,spjangcd);
		
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result; 
	}

}
