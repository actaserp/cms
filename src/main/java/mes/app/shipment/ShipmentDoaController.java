package mes.app.shipment;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import mes.app.shipment.enums.ShipmentStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.shipment.service.ShipmentDoaService;
import mes.domain.entity.Shipment;
import mes.domain.entity.ShipmentHead;
import mes.domain.entity.Suju;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.ShipmentHeadRepository;
import mes.domain.repository.ShipmentRepository;
import mes.domain.repository.SujuRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/shipment/shipment_do_a")
public class ShipmentDoaController {

	@Autowired
	private ShipmentDoaService shipmentDoaService;
	
	@Autowired
	ShipmentRepository shipmentRepository;
	
	@Autowired
	ShipmentHeadRepository shipmentHeadRepository;
	
	@Autowired
	SujuRepository sujuRepository;
	
	@GetMapping("/order_list")
	public AjaxResult getOrderList(
			@RequestParam("srchStartDt") String dateFrom,
			@RequestParam("srchEndDt") String dateTo,
			@RequestParam("cboCompany") String compPk,
			@RequestParam("cboMatGroup") String matGrpPk,
			@RequestParam("cboMaterial") String matPk,
			@RequestParam("not_ship") String notShip,
			@RequestParam("keyword") String keyword){
		
		List<Map<String, Object>> items = this.shipmentDoaService.getOrderList(dateFrom,dateTo,notShip,compPk,matGrpPk,matPk,keyword);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}

	@GetMapping("/shipment_item_list")
	public AjaxResult getShipmentItemList(
			@RequestParam("head_id") String headId){
		
		List<Map<String, Object>> items = this.shipmentDoaService.getShipmentItemList(headId);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@PostMapping("/batch_save")
	@Transactional
	public AjaxResult batchSave(
			@RequestBody MultiValueMap<String,Object> shiphead_list,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		List<Map<String, Object>> data = CommonUtil.loadJsonListMap(shiphead_list.getFirst("shiphead_list").toString());
		
		for(int i = 0; i < data.size(); i++) {
			Integer head_id = Integer.parseInt(data.get(i).get("id").toString());
			ShipmentHead smh = this.shipmentHeadRepository.getShipmentHeadById(head_id);
			
			if(!smh.getState().equals("shipped")) {
				List<Shipment> smList = shipmentRepository.findByShipmentHeadId(head_id);
				int orderSum = 0;
				
				for(int j = 0; j < smList.size(); j++) {
					//Shipment sm = new Shipment();
					//sm = this.shipmentRepository.getShipmentById(smList.get(j).getId());
					Shipment sm = smList.get(j);
					Double orderQty = smList.get(j).getOrderQty();
					sm.setQty(orderQty);
					sm.set_status("a");
					sm.set_audit(user);
					this.shipmentRepository.save(sm);
					
					orderSum += orderQty;

				}
				ShipmentStatus status = ShipmentStatus.SHIPPED;
				smh.setState(status.getLabel());
				smh.setTotalQty((float)orderSum);
				smh.set_audit(user);
				smh = this.shipmentHeadRepository.save(smh);
			}
			result.data = smh;
		}
		
		return result;
	}
	
	@PostMapping("/save_shipdata_a")
	@Transactional
	public AjaxResult savaShipdataA(
			@RequestParam("head_id") Integer headId,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		ShipmentHead smh = this.shipmentHeadRepository.getShipmentHeadById(headId);
		
		
		if (smh.getState().equals("shipped")) {
			return result;
		}
		
		List<Map<String, Object>> data = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		int orderSum = 0;
		
		for (int i = 0; i < data.size(); i++) {
			int orderQty = Integer.parseInt(data.get(i).get("order_qty").toString());
			Shipment sm = this.shipmentRepository.getShipmentById(Integer.parseInt(data.get(i).get("ship_pk").toString()));
			
			sm.setQty((double)orderQty);
			sm.set_status("a");
			sm.set_audit(user);
			this.shipmentRepository.save(sm);
			
			orderSum += orderQty;

			Object srcTableName = data.get(i).get("src_table_name");

			if(srcTableName != null && data.get(i).get("src_table_name").toString().equals("suju")) {
				Integer srcDataPk = Integer.parseInt(data.get(i).get("src_data_pk").toString());
				if (srcDataPk > 0) {
					Suju su = this.sujuRepository.getSujuById(srcDataPk);
					su.setShipmentState("shipped");
					su.set_audit(user);
				}
			}
			smh.setState("shipped");
			smh.setTotalQty((float)orderSum);
			smh.set_audit(user);
			smh = this.shipmentHeadRepository.save(smh);
		}
		result.data = smh;
		return result;
	}
}
