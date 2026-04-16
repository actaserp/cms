package mes.app.schedule;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.domain.entity.MatOrder;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.MatOrderRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/schedule/mat_order")
public class MatOrderController {
	
	@Autowired
	MatOrderRepository matOrderRepository;
	
	@PostMapping("/order_save")
	public AjaxResult matOrderSave(
			@RequestBody MultiValueMap<String,Object> mo,
			@RequestParam("status") String status,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		Timestamp today = new Timestamp(System.currentTimeMillis());
		
		List<Map<String, Object>> data = CommonUtil.loadJsonListMap(mo.getFirst("mo").toString());
		
		if (status.equals("registered")) {
			for (int i = 0; i < data.size(); i++) {
				Integer compPk = 0;
				Integer unitPrice = 0;
				
				compPk = Integer.parseInt(data.get(i).get("Company_id").toString());
				unitPrice = Integer.parseInt(data.get(i).get("UnitPrice").toString());
				
				if (compPk <= 0 && unitPrice <= 0 ) {
					continue;
				}
				Integer packOrderQty = 0;
				Integer packUnitQty = 0;
				
				packOrderQty = Integer.parseInt(data.get(i).get("AddQty").toString());
				packUnitQty = Integer.parseInt(data.get(i).get("PackingUnitQty").toString());
				
				if (packOrderQty <= 0 && packUnitQty <= 0) {
					continue;
				}
				Integer orderQty = packOrderQty * packUnitQty;
				Integer totalPrice = orderQty * unitPrice;
				
				Timestamp inputPlanDate = CommonUtil.tryTimestamp(data.get(i).get("InputPlanDate").toString());
				
				Integer materialRequirementId = null;
				
				if (data.get(i).containsKey("id")) {
					materialRequirementId = Integer.parseInt(data.get(i).get("id").toString());
				}
				
				MatOrder m = new MatOrder();
				m.setState("registered");
				m.setCompanyId(compPk);
				m.setMaterialId(Integer.parseInt(data.get(i).get("mat_pk").toString()));
				m.setInputPlanDate(inputPlanDate);
				m.setMaterialRequirementId(materialRequirementId);
				m.setUnitPrice((float)unitPrice);
				m.setPackOrderQty((float)packOrderQty);
				m.setOrderQty((float)orderQty);
				m.setTotalPrice((float)totalPrice);
				m.setOrderDate(today);
				m.set_audit(user);
				// 당시가용재고 Not null 조건으로 임시로 0 
				m.setAvailableStock((float)0);
				
				if (materialRequirementId == null) {
					m.set_status("urgent");
				}
				m = this.matOrderRepository.save(m);
				result.data = m;
				
				
			}
		} else if (status.equals("approved") || status.equals("rejected")) {
			for (int i = 0; i < data.size(); i++) {
				MatOrder m = this.matOrderRepository.getMatOrderById(data.get(i).get("id").toString());
				if(status.equals("approved")) {
					m.setState("approved");
				} else if(status.equals("rejected")) {
					m.setState("rejected");
				}
				m.setApproverId(user.getId());
				m.setApproveDateTime(today);
				m.set_audit(user);
				m = this.matOrderRepository.save(m);
				result.data = m;	
			}
		} 
		return result;
	}

}
