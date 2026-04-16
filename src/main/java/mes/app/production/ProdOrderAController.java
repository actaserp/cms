package mes.app.production;

import java.sql.Timestamp;
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

import mes.app.production.service.ProdOrderAService;
import mes.domain.entity.JobRes;
import mes.domain.entity.Material;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.JobResRepository;
import mes.domain.repository.MaterialRepository;
import mes.domain.repository.RoutingProcRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/production/prod_order_a")
public class ProdOrderAController {
	
	@Autowired
	private ProdOrderAService prodOrderAService;
	
	@Autowired
	MaterialRepository materialRepository;
	
	@Autowired
	JobResRepository jobResRepository;
	
	@Autowired
	RoutingProcRepository routingProcRepository;
	
	@GetMapping("/read")
	public AjaxResult getProdOrderA(
			@RequestParam(value="date_from", required = true) String dateFrom,
			@RequestParam(value="date_to", required = true) String dateTo,
			@RequestParam("workcenter_pk") String workcenterPk,
			@RequestParam("mat_type") String matType,
			@RequestParam("mat_grp_pk") String matGrpPk,
			@RequestParam("keyword") String keyword,
			@RequestParam("spjangcd") String spjangcd
			){
		List<Map<String, Object>> items = this.prodOrderAService.getProdOrderA(dateFrom,dateTo,matGrpPk,keyword,matType,workcenterPk,spjangcd);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}
	
	@GetMapping("/mat_info")
	public AjaxResult getMatInfo(
			@RequestParam("id") String id ) {
		
		Map<String, Object> items = this.prodOrderAService.getMatInfo(id);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/detail")
	public AjaxResult getProdOrderADetail(
			@RequestParam("jr_pk") String jrPk) {
		
		Map<String, Object> items = this.prodOrderAService.getProdOrderADetail(jrPk);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@PostMapping("/save")
	public AjaxResult saveProdOrderA(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam("production_date") String productionDate,
			@RequestParam(value = "cboEquipment", required=false) Integer cboEquipment,
			@RequestParam("cboMaterial") Integer cboMaterial,
			@RequestParam("cboMaterialGrp") Integer cboMaterialGrp,
			@RequestParam("cboShiftCode") String cboShiftCode,
			@RequestParam("cboWorcenter") Integer cboWorcenter,
			@RequestParam("txtDescription") String txtDescription,
			@RequestParam("txtOrderQty") Integer txtOrderQty,
			@RequestParam("txtUnit") String txtUnit,
			@RequestParam("spjangcd") String spjangcd,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		Integer jrPk = id;
		Integer matPk = cboMaterial;
		
		Material m = this.materialRepository.getMaterialById(matPk);
		Integer routingPk = m.getRoutingId();
		
		Integer locPk = m.getStoreHouseId();
		
		Integer routingId = 0;
		Integer processCnt;
		Timestamp prodDate = CommonUtil.tryTimestamp(productionDate);
		if (routingPk != null) {
			processCnt = this.routingProcRepository.countByRoutingId(routingPk);
			routingId = routingPk;
		} else {
			processCnt = 1;
		}
		
		JobRes jr = null;
		if (jrPk != null) {
			jr = this.jobResRepository.getJobResById(jrPk);
			if (!jr.getState().equals("ordered")) {
				result.success = false;
				result.message = "지시중인 상태가 아닙니다. \n 지시 상태에서만 수정할 수 있습니다.";
				return result;
			}
		} else {
			jr = new JobRes();
		}
		
		jr.set_audit(user);
		jr.setProductionDate(prodDate);
		jr.setProductionPlanDate(prodDate);
		jr.setShiftCode(cboShiftCode);
		jr.setMaterialId(matPk);
		jr.setOrderQty((float)txtOrderQty);
		jr.setWorkCenter_id(cboWorcenter);
		jr.setFirstWorkCenter_id(cboWorcenter);
		jr.setEquipment_id(cboEquipment);
		jr.setDescription(txtDescription);
		if (routingId > 0) {
			jr.setRouting_id(routingId);			
		}
		jr.setProcessCount(processCnt);
		jr.setStoreHouse_id(locPk);
		jr.setWorkIndex(1);
		jr.setLotCount(1);
		jr.setState("ordered");
		jr.setSpjangcd(spjangcd);
		
		jr = this.jobResRepository.save(jr);	
		
        result.data = jr;
		return result;
		
	}
	
	@PostMapping("/delete")
	@Transactional
	public AjaxResult deleteProdOrderA(@RequestParam("id") Integer id) {
		AjaxResult result = new AjaxResult();
		
		Map<String, Object> row = this.prodOrderAService.getJopResRow(id);
		
		if (row == null) {
			result.success = true;
			result.code = id.toString();
			return result;
		}
		
		Integer sujuPk = 0;
		if (row.get("state").equals("ordered")) {
			if (row.get("src_table") != null) {
				if (row.get("src_table").equals("suju")) {
					sujuPk = Integer.parseInt(row.get("src_pk").toString());
				}
			} else {
				sujuPk = 0;
			}
		}
		
		int deletYn = this.prodOrderAService.deleteById(id);
		
		if (sujuPk > 0 && deletYn > 0) {
			this.prodOrderAService.updateBySujuPk(sujuPk);
		}
		
		return result;
	}
}
