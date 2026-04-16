package mes.app.shipment;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.shipment.service.ShipmentListService;
import mes.domain.model.AjaxResult;

@RestController
@RequestMapping("/api/shipment/shipment_list")
public class ShipmentListController {
	
	@Autowired
	private ShipmentListService shipmentListService;
	
	@GetMapping("/shipment_head_list")
	public AjaxResult getShipmentHeadList(
			@RequestParam("srchStartDt") String dateFrom,
			@RequestParam("srchEndDt") String dateTo,
			@RequestParam("cboCompany") String compPk,
			@RequestParam("cboMatGroup") String matGrpPk,
			@RequestParam("cboMaterial") String matPk,
			@RequestParam("keyword") String keyword
	) {
		
		List<Map<String, Object>> items = this.shipmentListService.getShipmentHeadList(dateFrom,dateTo,compPk,matGrpPk,matPk,keyword, "shipped");
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/shipment_item_list")
	public AjaxResult getShipmentItemList(
			@RequestParam("head_id") String headId
			) {
		
		List<Map<String, Object>> items = this.shipmentListService.getShipmentItemList(headId, null);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
}
