package mes.app.inventory;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.inventory.service.MaterialCurrentStockService;
import mes.domain.model.AjaxResult;

@RestController
@RequestMapping("/api/inventory/material_current_stock")
public class MaterialCurrentStockController {

	@Autowired
	private MaterialCurrentStockService MaterialCurrentStockService;
	
	// 재고 현황 조회
	@GetMapping("/read")
	public AjaxResult getMaterialCurrentStockList(
			@RequestParam(value="mat_type", required=false) String mat_type,
			@RequestParam(value="mat_grp_pk", required=false) Integer mat_grp_pk,
			@RequestParam(value="mat_name", required=false) String mat_name,
			@RequestParam(value="store_house_id", required=false) Integer store_house_id,
			@RequestParam(value="spjangcd", required=false) String spjangcd,
			HttpServletRequest request) {
		
		List<Map<String, Object>> items = this.MaterialCurrentStockService.getMaterialCurrentStockList(mat_type, mat_grp_pk, mat_name, store_house_id, spjangcd);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
}
