package mes.app.inventory;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.inventory.service.MatStockTakeHistoryService;
import mes.domain.model.AjaxResult;

@RestController
@RequestMapping("/api/inventory/mat_stock_take_history")
public class MatStockTakeHistoryController {

	@Autowired
	MatStockTakeHistoryService matStockTakeHistoryService;

	// 조회
	@GetMapping("/read")
	public AjaxResult getMatStockTakeHistoryList(
			@RequestParam(value = "date_from", required = false) String date_from,
			@RequestParam(value = "date_to", required = false) String date_to,
			@RequestParam(value = "house_pk", required = false) Integer house_pk,
			@RequestParam(value = "mat_name", required = false) String mat_name,
			@RequestParam(value = "mat_type", required = false) String mat_type,
			@RequestParam(value = "manage_level", required = false) String manage_level,
			@RequestParam("spjangcd") String spjangcd,
			@RequestParam(value = "mat_group_pk", required = false) Integer mat_group_pk) {
		        
		List<Map<String, Object>> items = this.matStockTakeHistoryService.getMatStockTakeHistoryList(date_from, date_to, house_pk, mat_name, mat_type, manage_level, mat_group_pk, spjangcd);
		 
		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}
}
