package mes.app.maintenance;

import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.maintenance.service.EquipmentMaintService;
import mes.domain.entity.EquipmentMaint;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.EquipmentMaintRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/maintenance/equipment_maint")
public class EquipmentMaintController {

	@Autowired
	private EquipmentMaintService equipMaintService;
	
	@Autowired
	EquipmentMaintRepository equipmentMaintRepository;
	
	/**
	 * 설비정비 조회
	 * @param start_date
	 * @param end_date
	 * @param equip_id
	 * @return
	 */
	@GetMapping("/read")
	public AjaxResult getEquipmentMaintList(
			@RequestParam("srchStartDt") String start_date,
			@RequestParam("srchEndDt") String end_date,
			@RequestParam("cboEquipment") String equip_id) {
		
		List<Map<String, Object>> items = this.equipMaintService.getEquipmentMaintList(start_date,end_date,equip_id);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	/**
	 * 설비정비 상세조회
	 * @param id
	 * @return
	 */
	@GetMapping("/detail")
	public AjaxResult getEquipmentMaintDetail(
			@RequestParam("id") int id) {
	
		Map<String, Object> items = this.equipMaintService.getEquipmentMaintDetail(id);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	/**
	 * 설비정비 저장(예방정비,고장정비)
	 * @param id
	 * @param equipment_id
	 * @param maint_type
	 * @param maint_start_date
	 * @param maint_end_date
	 * @param maint_start_time
	 * @param maint_end_time
	 * @param servicer_name
	 * @param maint_cost
	 * @param description
	 * @param fail_start_date
	 * @param fail_end_date
	 * @param fail_start_time
	 * @param fail_end_time
	 * @param fail_hr
	 * @param fail_description
	 * @return
	 */
	
	@PostMapping("/save")
	public AjaxResult saveEquipmentMaint(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam("equipment_id") int equipment_id,
			@RequestParam("maint_type") String maint_type,
			@RequestParam("maint_start_date") String maint_start_date,
			@RequestParam("maint_end_date") String maint_end_date,
			@RequestParam("maint_start_time") String maint_start_time,
			@RequestParam("maint_end_time") String maint_end_time,
			@RequestParam("service_name") String servicer_name,
			@RequestParam("maint_cost") Object maint_cost,
			@RequestParam("description") String description,
			@RequestParam(value="fail_start_date", required=false) String fail_start_date,
			@RequestParam(value="fail_end_date", required=false) String fail_end_date,
			@RequestParam(value="fail_start_time", required=false) String fail_start_time,
			@RequestParam(value="fail_end_time", required=false) String fail_end_time,
			@RequestParam(value="fail_hr", required=false) Object fail_hr,
			@RequestParam(value="fail_description", required=false) String fail_description,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		EquipmentMaint em = null;
		
		Timestamp MaintStartDate = CommonUtil.tryTimestamp(maint_start_date);
		Timestamp MaintEndDate = CommonUtil.tryTimestamp(maint_end_date);
		LocalTime MaintStartTime = LocalTime.parse(maint_start_time);
		LocalTime MaintEndTime = LocalTime.parse(maint_end_time);
		
		Timestamp failStartDate = fail_start_date != null ? CommonUtil.tryTimestamp(fail_start_date) : null;
		Timestamp failEndDate = fail_end_date != null ? CommonUtil.tryTimestamp(fail_end_date) : null;
		LocalTime failStartTime = fail_start_time != null && !fail_start_time.isEmpty() ? LocalTime.parse(fail_start_time) : null;
		LocalTime failEndTime = fail_end_time != null && !fail_end_time.isEmpty() ? LocalTime.parse(fail_end_time) : null;
		
		Timestamp today = new Timestamp(System.currentTimeMillis());
		
		if (id==null) {
			em = new EquipmentMaint();
		} else {
			//this.equipMaintService.deleteFailure(id);
			em = this.equipmentMaintRepository.getEquipmentMaintById(id);
		}
		
		em.setDataDate(today);
		em.setEquipment_id(equipment_id);
		em.setMaintType(maint_type);
		em.setMaintStartDate(MaintStartDate);
		em.setMaintEndDate(MaintEndDate);
		em.setMaintStartTime(MaintStartTime);
		em.setMaintEndTime(MaintEndTime);
		em.setServicerName(servicer_name);
		em.setMaintCost(CommonUtil.tryFloatNull(maint_cost));
		em.setDescription(description);
		em.setFailStartDate(failStartDate);
		em.setFailEndDate(failEndDate);
		em.setFailStartTime(failStartTime);
		em.setFailEndTime(failEndTime);
		em.setFailHr(CommonUtil.tryFloatNull(fail_hr));
		em.setFailDescription(CommonUtil.tryString(fail_description));
		em.set_audit(user);
		em = this.equipmentMaintRepository.save(em);
		
		AjaxResult result = new AjaxResult();
        result.data = em;
		return result;
	}
	
	/**
	 * 설비정비 삭제
	 * @param id
	 * @return
	 */
	@PostMapping("/delete")
	public AjaxResult deleteEquipmentMaint(@RequestParam("id") Integer id) {
		this.equipmentMaintRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		return result;
	}
}
