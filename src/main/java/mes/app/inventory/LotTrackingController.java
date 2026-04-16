package mes.app.inventory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.inventory.service.LotService;
import mes.domain.model.AjaxResult;

@RestController
@RequestMapping("/api/lot/lot_tracking")
public class LotTrackingController {
	
	@Autowired
	public LotService lotService;
	
	@GetMapping("/lot_detail")
	public AjaxResult lotDetail(
			@RequestParam("lot_number") String lotNumber) {
		
		List<Map<String, Object>> items = this.lotService.lotDetail(lotNumber);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}
	
	@GetMapping("/lot_history")
	public AjaxResult lotHistory(
			@RequestParam("mat_type") String matType,
			@RequestParam("lot_number") String lotNumber) {
		
		Map<String, Object> items = new HashMap<>();
		List<Map<String, Object>> m1 = lotService.getMaterialTracking(lotNumber);
		List<Map<String, Object>> m2 = lotService.getProductTracking(lotNumber);
		List<Map<String, Object>> m3 = lotService.getMaterialInoutTracking(lotNumber);
		List<Map<String, Object>> m4 = lotService.getProductShipmentTracking(lotNumber);
		items.put("m_item", m1);
		items.put("p_item", m2);
		items.put("inout_list", m3);
		items.put("shipment_list", m4);
		AjaxResult result = new AjaxResult();
		result.data =  items;
		return result;
	}
}
