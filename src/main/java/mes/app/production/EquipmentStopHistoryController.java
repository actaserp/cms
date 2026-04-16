package mes.app.production;

import java.sql.Timestamp;
import java.util.HashMap;
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

import mes.app.definition.service.EquipmentService;
import mes.domain.entity.EquRun;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.EquRunRepository;

@RestController
@RequestMapping("/api/production/equipment_stop_history")
public class EquipmentStopHistoryController {

	@Autowired
	private EquipmentService equipmentService;
	
	@Autowired
	EquRunRepository equRunRepository;
	
	@GetMapping("/read")
	public AjaxResult getEquipmentStopList(
			@RequestParam(value="date_from", required=false) String dateFrom, 
			@RequestParam(value="date_to", required=false) String dateTo,
			@RequestParam(value="equipment", required=false) String equipment,
			HttpServletRequest request
			) {
		
		AjaxResult result = new AjaxResult();
		
		List<Map<String,Object>> items = this.equipmentService.getEquipmentStopList(dateFrom,dateTo,equipment);
		
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/detail")
	public AjaxResult getEquipmentStopInfo(
			@RequestParam(value="id", required=false) Integer id, 
			@RequestParam(value="runType", required=false) String runType,
			HttpServletRequest request) {
		
		AjaxResult result = new AjaxResult();
		
		Map<String,Object> items = this.equipmentService.getEquipmentStopInfo(id,runType);
		
		result.data = items;
		
		return result;
	}
	
	
	@PostMapping("/addData")
	public AjaxResult addData(
			@RequestParam(value="id", required=false) Integer id, 
			@RequestParam(value="Equipment_id", required=false) Integer equipmentId,
			@RequestParam(value="start_date", required=false) String start_date, 
			@RequestParam(value="StartTime", required=false) String startTime,
			@RequestParam(value="end_date", required=false) String end_date, 
			@RequestParam(value="EndTime", required=false) String endTime,
			@RequestParam(value="Description", required=false) String description, 
			@RequestParam(value="StopCause_id", required=false) Integer stopCauseId,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
        Timestamp startDate = Timestamp.valueOf(start_date+ " " + startTime + ":00");
        Timestamp endDate = Timestamp.valueOf(end_date+ " " + endTime + ":00");
        
        Integer equRunId = null;
        
        EquRun er = new EquRun();
        
        if(id != null) {
        	er = this.equRunRepository.getEquRunById(id);
        } else {
        	er.setRunState("X");
        }
        er.setEquipmentId(equipmentId);
        er.setStartDate(startDate);
        er.setEndDate(endDate);
        er.setDescription(description);
        er.setStopCauseId(stopCauseId);
        er.set_audit(user);
        
        this.equRunRepository.save(er);
        
        equRunId = er.getId(); 
        
		AjaxResult result = new AjaxResult();
		
		Map<String,Object> item = new HashMap<>();
		
		item.put("id", equRunId);
		
		result.data = item;
		
		return result;
	}
	
	@PostMapping("/delData")
	public AjaxResult delData(
			@RequestParam(value="id", required=false) Integer id) {
		
		AjaxResult result = new AjaxResult();
		if (id != null) {
			this.equRunRepository.deleteById(id);
		}
		return result;
	}
	
}
