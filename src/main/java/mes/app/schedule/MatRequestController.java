package mes.app.schedule;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.schedule.service.MatRequestService;
import mes.domain.model.AjaxResult;

@RestController
@RequestMapping("/api/schedule/mat_request")
public class MatRequestController {
	
	@Autowired
	private MatRequestService matRequestService;
	
	@GetMapping("/read")
	public AjaxResult getMatRequestList(
			@RequestParam("cboMatGrp") String cboMatGrp,
			@RequestParam("cboMaterial") String cboMaterial,
			@RequestParam("chkSearchDate") String chkSearchDate,
			@RequestParam("srchEndDt") String srchEndDt,
			@RequestParam("srchStartDt") String srchStartDt) {
		
		List<Map<String, Object>> items = this.matRequestService.getMatRequestList(cboMatGrp,cboMaterial,chkSearchDate,srchEndDt,srchStartDt);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	// 긴급발주 팝업에서 품목 변경시
	@GetMapping("/company_by_mat")
	public AjaxResult getCompanyByMat(
			@RequestParam("mat_pk") String matPk,
			@RequestParam(value="comp_pk", required = false) String compPk) {
		
		List<Map<String, Object>> items = this.matRequestService.getCompanyByMat(matPk,compPk);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	// 긴급발주 팝업에서 구매처 변경시
	@GetMapping("/unit_price")
	public AjaxResult getUnitPrice(
			@RequestParam("mat_pk") String matPk,
			@RequestParam(value="comp_pk", required = false) String compPk) {
		
		Map<String, Object> items = this.matRequestService.getUnitPrice(matPk,compPk);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}

}
