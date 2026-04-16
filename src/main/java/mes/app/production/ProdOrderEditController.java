package mes.app.production;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.production.service.ProdOrderEditService;
import mes.domain.entity.JobRes;
import mes.domain.entity.Material;
import mes.domain.entity.Suju;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.JobResRepository;
import mes.domain.repository.MaterialRepository;
import mes.domain.repository.RoutingProcRepository;
import mes.domain.repository.SujuRepository;

@RestController
@RequestMapping("/api/production/prod_order_edit")
public class ProdOrderEditController {

	@Autowired
	private ProdOrderEditService prodOrderEditService;

	@Autowired
	MaterialRepository materialRepository;
	
	@Autowired
	RoutingProcRepository routingProcRepository;
	
	@Autowired
	JobResRepository jobResRepository;
	
	@Autowired
	SujuRepository sujuRepository;
	
	// 수주 목록 조회
	@GetMapping("/suju_list")
	public AjaxResult getSujuList(
			@RequestParam(value="date_kind", required=false) String date_kind,
			@RequestParam(value="start", required=false) String start,
			@RequestParam(value="end", required=false) String end,
			@RequestParam(value="mat_group", required=false) Integer mat_group,
			@RequestParam(value="mat_name", required=false) String mat_name,
			@RequestParam("spjangcd") String spjangcd,
			@RequestParam(value="not_flag", required=false) String not_flag) {

		List<Map<String, Object>> items = this.prodOrderEditService.getSujuList(date_kind, start, end, mat_group, mat_name, not_flag, spjangcd);
		
        AjaxResult result = new AjaxResult();
        result.data = items;
		
        return result;
	}

	// 제품 지시내역 조회
	@GetMapping("/joborder_list")
	public AjaxResult getJobOrderList(
			@RequestParam(value="suju_id", required=false) Integer suju_id) {

		List<Map<String, Object>> items = this.prodOrderEditService.getJobOrderList(suju_id);
		
        AjaxResult result = new AjaxResult();
        result.data = items;
		
        return result;
	}

	// 제품 지시내역 상세조회
	@GetMapping("/joborder_detail")
	public AjaxResult getJobOrderDetail(
			@RequestParam("jobres_id") Integer jobres_id,
			HttpServletRequest request) {
		
		Map<String, Object> item = this.prodOrderEditService.getJobOrderDetail(jobres_id);
		
		AjaxResult result = new AjaxResult();
		result.data = item;
		
		return result;
	}
	
	// 반제품 작업지시 조회
	@GetMapping("/semi_list")
	public AjaxResult getSemiList(
			@RequestParam(value="data_date", required=false) String data_date,
			@RequestParam(value="mat_pk", required=false) Integer mat_pk,
			@RequestParam(value="suju_qty", required=false) Integer suju_qty,
			@RequestParam(value="suju_pk", required=false) Integer suju_pk) {
		
		List<Map<String, Object>> items = this.prodOrderEditService.getSemiList(data_date, mat_pk, suju_qty, suju_pk);
		
        AjaxResult result = new AjaxResult();
        result.data = items;
		
        return result;
	}
		
	// 반제품 지시내역 조회
	@GetMapping("/semi_joborder_list")
	public AjaxResult getSemiJoborderList(
			@RequestParam(value="suju_id", required=false) Integer suju_id) {

		List<Map<String, Object>> items = this.prodOrderEditService.getSemiJoborderList(suju_id);
		
        AjaxResult result = new AjaxResult();
        result.data = items;
		
        return result;
	}
	
	// 작업지시 생성
	@PostMapping("/make_prod_order")
	@Transactional
	public AjaxResult makeProdOrder(
			@RequestParam(value="suju_id", required=false) Integer sujuId,
			@RequestParam(value="prod_date", required=false) String prodDate,
			@RequestParam(value="Material_id", required=false) Integer materialId,
			@RequestParam(value="workshift", required=false) String workShift,
			@RequestParam(value="workcenter_id", required=false) Integer workcenterId,
			@RequestParam(value="equ_id", required=false) Integer equipmentId,
			@RequestParam(value="AdditionalQty", required=false) Float additionalQty,
			@RequestParam("spjangcd") String spjangcd,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();
		
		User user = (User)auth.getPrincipal();
		
		Material m = this.materialRepository.getMaterialById(materialId);
		
		Integer routingPk = m.getRoutingId();
		Integer locPk = m.getStoreHouseId();
		Integer routingId = null;
		Integer processCount = null;
		
		if (routingPk != null) {
			processCount = this.routingProcRepository.countByRoutingId(routingPk);
			routingId = routingPk;
		} else {
			routingId = null;
		}
		
		Timestamp prod_date = Timestamp.valueOf(prodDate + " 00:00:00");
		
		// 작업지시 번호는 trigger에서 자동으로 생성된다
		JobRes jr = new JobRes();
		
		jr.setSourceDataPk(sujuId);
		jr.setSourceTableName("suju");
		jr.setState("ordered");
		jr.setMaterialId(materialId);
		jr.setOrderQty(additionalQty);
		jr.setProductionDate(prod_date);
		jr.setProductionPlanDate(prod_date);
		jr.setWorkCenter_id(workcenterId);
		
		if (equipmentId != null) {
			jr.setEquipment_id(equipmentId);
		} else {
			jr.setEquipment_id(m.getEquipment());
		}
		
		jr.setFirstWorkCenter_id(workcenterId);
		
		jr.setRouting_id(routingId);
		jr.setProcessCount(processCount);
		jr.setStoreHouse_id(locPk);
		jr.set_audit(user);
		jr.setWorkIndex(1);
		jr.setSpjangcd(spjangcd);

		if (workShift != null) {
			jr.setShiftCode(workShift);
		}
		
		jr = this.jobResRepository.save(jr);
		
		List<Map<String, Object>> list = this.prodOrderEditService.makeProdOrder(sujuId);
		
		for (int i = 0; i < list.size(); i++) {
			Integer pk = Integer.parseInt(list.get(i).get("suju_id").toString());
			if(Float.parseFloat(list.get(i).get("remain_qty").toString()) == (float)0) {
				Suju s = this.sujuRepository.getSujuById(pk);
				s.setState("ordered");
				s.set_audit(user);
				s = this.sujuRepository.save(s);
			}
		}
		
		Map<String,Object> item = new HashMap<String,Object>();
		item.put("jobres_id", jr.getId());
		item.put("info", list);
		
		result.success = true;
		result.data = item;
		
		return result;
	}

	// 지시내역 수정
	@PostMapping("/update_order")
	@Transactional
	public AjaxResult updateOrder(
			@RequestParam(value="id", required=false) Integer jobres_id,
			@RequestParam(value="ProductionDate", required=false) String productionDate,
			@RequestParam(value="ShiftCode", required=false) String ShiftCode,
			@RequestParam(value="WorkCenter_id", required=false) Integer WorkCenter_id,
			@RequestParam(value="Equipment_id", required=false) Integer Equipment_id,
			@RequestParam(value="OrderQty", required=false) Float OrderQty,
			@RequestParam(value="Description", required=false) String Description,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();
		
		User user = (User)auth.getPrincipal();
		
		Timestamp ProductionDate = Timestamp.valueOf(productionDate + " 00:00:00");
		
		JobRes jr = this.jobResRepository.getJobResById(jobres_id);
		
		if (jr != null) {
			
			jr.setProductionDate(ProductionDate);
			jr.setShiftCode(ShiftCode);
			jr.setWorkCenter_id(WorkCenter_id);
			jr.setOrderQty(OrderQty);
			jr.setDescription(Description);
			if (Equipment_id != null) {
				jr.setEquipment_id(Equipment_id);
			}
			jr.set_audit(user);

			jr = this.jobResRepository.save(jr);
						
			result.success = true;
		} else {
			result.success = false;					
		}
		
		return result;
	}
	
}
