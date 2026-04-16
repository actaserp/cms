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

import mes.app.maintenance.service.EquipFailureService;
import mes.domain.entity.EquipmentMaint;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.EquipmentMaintRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/maintenance/equip_failure")
public class EquipFailureController {

	@Autowired
	private EquipFailureService equipFailureService;
	
	@Autowired
	EquipmentMaintRepository equipmentMaintRepository;
	/**
	 * 설비 고장 리스트 조회
	 * @param start_date
	 * @param end_date
	 * @param equip_id
	 * @return
	 */
	@GetMapping("/read")
	public AjaxResult getEquipFailureList(
			@RequestParam("start_date") String start_date,
			@RequestParam("end_date") String end_date,
			@RequestParam("equip_id") String equip_id) {
		
		List<Map<String, Object>> items = this.equipFailureService.getEquipFailureList(start_date,end_date,equip_id);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	/**
	 * 설비고장 상세조회
	 * @param id
	 * @return
	 */
	@GetMapping("/detail")
	public AjaxResult getEquipmentMaintDetail(
			@RequestParam("id") int id) {
	
		Map<String, Object> items = this.equipFailureService.getEquipmentMaintDetail(id);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	
	/**
	 * 설비고장 저장
	 * @param id
	 * @param equipment_id
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
			@RequestParam("fail_start_date") String fail_start_date,
			@RequestParam("fail_end_date") String fail_end_date,
			@RequestParam("fail_start_time") String fail_start_time,
			@RequestParam("fail_end_time") String fail_end_time,
			@RequestParam("fail_hr") Float fail_hr,
			@RequestParam("fail_description") String fail_description,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		EquipmentMaint em = null;
		
		Timestamp failStartDate = CommonUtil.tryTimestamp(fail_start_date);
		Timestamp failEndDate = CommonUtil.tryTimestamp(fail_end_date);
		LocalTime failStartTime = LocalTime.parse(fail_start_time);
		LocalTime failEndTime = LocalTime.parse(fail_end_time);
		Timestamp today = new Timestamp(System.currentTimeMillis());
		
		if (id==null) {
			em = new EquipmentMaint();
		} else {
			em = this.equipmentMaintRepository.getEquipmentMaintById(id);
		}
		
		em.setMaintType("failure");
		em.setDataDate(today);
		em.setEquipment_id(equipment_id);
		em.setFailStartDate(failStartDate);
		em.setFailEndDate(failEndDate);
		em.setFailStartTime(failStartTime);
		em.setFailEndTime(failEndTime);
		em.setFailHr(fail_hr);
		em.setFailDescription(fail_description);
		em.set_audit(user);
		em = this.equipmentMaintRepository.save(em);
		
		AjaxResult result = new AjaxResult();
        result.data = em;
		return result;
	}
	
	/**
	 * 설비고장 삭제
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
