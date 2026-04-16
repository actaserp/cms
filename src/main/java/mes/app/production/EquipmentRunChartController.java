package mes.app.production;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.util.LinkedCaseInsensitiveMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.production.service.EquipmentRunChartService;
import mes.domain.entity.EquRun;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.EquRunRepository;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/production/equipment_run_chart")
public class EquipmentRunChartController {
	
	@Autowired
	SqlRunner sqlRunner;
	
	@Autowired
	EquipmentRunChartService equipmentRunChartService;
	
	@Autowired
	EquRunRepository equRunRepository;

	// 차트 searchMainData
	@GetMapping("/read")
	public AjaxResult getEquipmentRunChart(
			@RequestParam(value="date_from", required=false) String date_from, 
    		@RequestParam(value="date_to", required=false) String date_to,
    		@RequestParam(value="id", required=false) Integer id,
    		@RequestParam(value="runType", required=false) String runType,
			@RequestParam String spjangcd) {

		List<Map<String, Object>> items = this.equipmentRunChartService.getEquipmentRunChart(date_from, date_to, id, runType, spjangcd);
		List<Map<String, Object>> result = new ArrayList<>();

		Map<String, List<Map<String, Object>>> GroupByNameItems = items.stream()
				.filter(item -> item.get("Name") != null)
				.collect(Collectors.groupingBy(item -> item.get("Name").toString()));

		for(Map.Entry<String, List<Map<String, Object>>> entry : GroupByNameItems.entrySet()){

			List<Map<String, Object>> groupItems = entry.getValue();

			for (int i = 0; i < groupItems.size(); i++) {
				Map<String, Object> uptime = groupItems.get(i);

				Map<String, Object> Downtime = new HashMap<>(); //비가동시간

				//uptime.get("")
				Object endDate = uptime.get("end_date");
				Object endTime = uptime.get("EndTime");
				Object EquipmentName = uptime.get("Name");
				Object Equipment_id = uptime.get("Equipment_id");
				Object StopCause = uptime.get("StopCauseName");

				Timestamp StartDate = (Timestamp) uptime.get("StartDate");
				Timestamp EndDate = (Timestamp) uptime.get("EndDate");

				if(EndDate != null){
					long diffMillis = EndDate.getTime() - StartDate.getTime();
					long diffMinutes = (diffMillis / 1000) / 60;
					uptime.put("Runtime", String.valueOf(diffMinutes));
				}

				uptime.put("RunState", "run");
				uptime.put("StopCauseName", "");

				Map<String, Object> nextItem = (i + 1 < groupItems.size()) ? groupItems.get(i + 1) : null;

				result.add(uptime);
				if (EndDate == null) continue;

				Downtime.put("RunState", "stop");
				Downtime.put("Name", EquipmentName);
				Downtime.put("Equipment_id", Equipment_id);
				Downtime.put("start_date", endDate);
				Downtime.put("StartTime", endTime);
				Downtime.put("StopCauseName", StopCause);
				if(nextItem == null){
					Downtime.put("end_date", "");
					Downtime.put("EndTime", "");
					Downtime.put("Runtime", "");
				}else{
					Downtime.put("end_date", nextItem.get("start_date"));
					Downtime.put("EndTime", nextItem.get("StartTime"));

					Timestamp DownTimeEndDate = (Timestamp) nextItem.get("StartDate"); //비가동의 종료시간
					long diffMillis = DownTimeEndDate.getTime() - EndDate.getTime(); //가동되지 않았던 시간
					long diffMinutes = (diffMillis / 1000) / 60;

					Downtime.put("GapTime", diffMinutes);
				}
				result.add(Downtime);
			}
		}

		AjaxResult result2 = new AjaxResult();
        result2.data = result;
		return result2;
	}
	
	/*// 차트 fillData
	@GetMapping("/readData")
	public AjaxResult getEquipmentRunChart(
    		@RequestParam(value="id", required=false) Integer id,
    		@RequestParam(value="runType", required=false) String runType,
			HttpServletRequest request) {
		
		List<Map<String, Object>> items = this.equipmentRunChartService.getEquipmentRunChart(null, null, id, runType);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}*/
	
	// saveData
	@PostMapping("/addData")
	public AjaxResult addDataEquipmentRunChart (
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam(value="spjangcd") String spjangcd,
			@RequestParam(value="Equipment_id", required=false) Integer Equipment_id,
			@RequestParam(value="start_date", required=false) String start_date,
			@RequestParam(value="StartTime", required=false) String StartTime,
			@RequestParam(value="end_date", required=false) String end_date,
			@RequestParam(value="EndTime", required=false) String EndTime,
			@RequestParam(value="RunState", required=false) String RunState,
			@RequestParam(value="Description", required=false) String Description,
			@RequestParam(value="StopCause_id", required=false) Integer StopCause_id,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
		Timestamp startDate = Timestamp.valueOf(start_date + ' ' + StartTime + ":59");
		Timestamp endDate = Timestamp.valueOf(end_date + ' ' + EndTime + ":59");
		
		EquRun er = null;

		List<Map<String, Object>> overlappinged = equipmentRunChartService.OverlappingTimeQuery(startDate, endDate, Equipment_id, spjangcd);//현재 겹치는 시간은 안됨. 설비가 겹치는 시간대로 가동되는건 말이 안되기 때문


		if(!overlappinged.isEmpty()){
			boolean hasMultipleRecords = overlappinged.size() != 1;
			boolean hasEndDate = overlappinged.get(0).get("EndDate") != null;

			if(hasMultipleRecords || hasEndDate){
				result.success = false;
				result.message = "해당 시간대에 이미 가동 중인 기록이 있습니다.";
				return result;
			}
		}

		result.success = true;
		result.message = "성공!";
		return result;

		/*if (id==null) {
			er = new EquRun();
		} else {
			er = this.equRunRepository.getEquRunById(id);
		}
		
		er.setEquipmentId(Equipment_id);
		er.setStartDate(startDate);
		er.setEndDate(endDate);
		er.setRunState("run");
		er.setDescription(Description);
		er.setStopCauseId(StopCause_id);
		er.set_audit(user);
		er.setSpjangcd(spjangcd);

		this.equRunRepository.save(er);
		
		result.success = true;
		result.message = "저장하였습니다.";
		result.data = er.getId();
	    return result;*/
	}
	
	// delDataBind
	@PostMapping("/delData")
	public AjaxResult deleteEquipmentRunChart(
			@RequestParam("id") Integer id) {
		
		this.equRunRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		return result;
	}
}
