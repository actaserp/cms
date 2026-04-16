package mes.app.haccp;

import java.sql.Date;
import java.sql.Time;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.haccp.service.HaccpDiaryService;
import mes.app.haccp.service.HaccpProcessService;

import mes.domain.entity.HaccpDiary;
import mes.domain.entity.HaccpDiaryDeviationDetect;
import mes.domain.entity.HaccpTest;
import mes.domain.entity.Material;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.ApprResultRepository;
import mes.domain.repository.HaccpDiaryDeviationDetectRepository;
import mes.domain.repository.HaccpDiaryRepository;
import mes.domain.repository.HaccpItemRepository;
import mes.domain.repository.HaccpItemResultRepository;
import mes.domain.repository.HaccpTestRepository;
import mes.domain.repository.MaterialRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.DateUtil;
import mes.domain.services.LogWriter;

@RestController
@RequestMapping("/api/haccp/diary")
public class HaccpDiaryController {

	@Autowired
	ApprResultRepository apprResultRepository;
	
	@Autowired
	HaccpDiaryService haccpDiaryService;

	@Autowired
	HaccpDiaryRepository haccpDiaryRepository;
	
	@Autowired
	HaccpTestRepository haccpTestRepository;
	
	@Autowired
    HaccpItemRepository haccpItemRepository;
	
	@Autowired
	HaccpItemResultRepository haccpItemResultRepository;
	
	@Autowired
	HaccpDiaryDeviationDetectRepository haccpDiaryDeviationDetectRepository;
	
	@Autowired
	private HaccpProcessService haccpProcessService;
	
	@Autowired 
	MaterialRepository materialRepository;
	
	
	@Autowired
	LogWriter logWriter;
	

	// 조회
	@GetMapping("/read")
	public AjaxResult getDiaryList(@RequestParam(value = "srchStartDt", required = true) String startDate,
			@RequestParam(value = "srchEndDt", required = true) String endDate,
			@RequestParam(value = "hp_id", required = true) Integer hp_id,
			@RequestParam(value = "task_code", required = true) String taskCode, 
			HttpServletRequest request) {
		
		List<Map<String, Object>> items = this.haccpDiaryService.getDiaryList(startDate, endDate,  hp_id, taskCode);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}

	// 일지 상세
	@GetMapping("/detail")
	public AjaxResult getDiaryDetail(@RequestParam(value = "hd_id", required = false) Integer hd_id,
			HttpServletRequest request) {

		Map<String, Object> dicDiary = this.haccpDiaryService.getDiaryDetail(hd_id);
		List<Map<String, Object>> items = this.haccpDiaryService.getHaccpTestResultItemTreeList(hd_id);
		dicDiary.put("items", items);

		AjaxResult result = new AjaxResult();
		result.data = dicDiary;
		return result;
	}

	@PostMapping("/delete")
	public AjaxResult deleteDiary(@RequestParam(value="hd_id", required=true) int id) {
		AjaxResult result = new AjaxResult();
		this.haccpDiaryRepository.deleteById(id);
		return result;
	}

	@GetMapping("/haccp_process")
	public AjaxResult getHaccpProcessDetail(@RequestParam(value = "hp_id", required = false) Integer hp_id,
			HttpServletRequest request) {
		Map<String, Object> item = this.haccpProcessService.getHaccpProcessDetail(hp_id); // haccp_proc detail
		AjaxResult result = new AjaxResult();
		result.data = item;
		return result;
	}

	@PostMapping("/save")
	public AjaxResult saveDiary(@RequestParam(value = "hd_id", required = false) Integer hd_id,
			@RequestParam(value = "hp_id", required = true) int hp_id,
			@RequestParam(value = "data_date", required = true) String dataDate,
			HttpServletRequest request,
			Authentication auth
			) {

		User user = (User)auth.getPrincipal();
		AjaxResult result = new AjaxResult();
		HaccpDiary haccpDairy = new HaccpDiary();
		
		if(hd_id==0 || hd_id ==null) {
			int count  = this.haccpDiaryService.getHaccpDiaryCountByDataDateAndHaccpProcess(dataDate, hp_id);
			if(count>0) {
				result.success=false;
				result.message = "이미 작성된 일지입니다.";
				return result;
			}
		}else {
			haccpDairy.setId(hd_id);
		}
		String username = user.getUserProfile().getName();
		Date date = Date.valueOf(dataDate);
		haccpDairy.setHaccpProcess_id(hp_id);
		haccpDairy.setDataDate(date);
		haccpDairy.setActionUserName(username);
		haccpDairy.set_audit(user);
		this.haccpDiaryRepository.save(haccpDairy);

		result.data = haccpDairy;
		return result;
	}
	
	@PostMapping("/save_haccp_test")
	public AjaxResult saveHaccpTest(@RequestParam(value = "hd_id", required = true) int hd_id,
			@RequestParam(value = "equipment_id", required = true) int equipment_id,
			@RequestParam(value = "material_id", required = false) Integer material_id,
			@RequestParam(value = "start_time", required = true) String start_time,
			@RequestParam(value = "data_type", required = true) String dataType,
			HttpServletRequest request,
			Authentication auth
			) {
		User user = (User)auth.getPrincipal();
		AjaxResult result = new AjaxResult();
		
		
		
		if(StringUtils.hasText(start_time)==false) {
			start_time = DateUtil.getHHmmByTodayString();
		}
		start_time = start_time + ":00";
		Time startTime = Time.valueOf(start_time);
		
		HaccpTest haccpTest = new HaccpTest();
		haccpTest.setHaccpDiaryId(hd_id);
		haccpTest.setEquipmentId(equipment_id);
		haccpTest.setStartTime(startTime);
		haccpTest.setEndTime(startTime);
		haccpTest.setDataType(dataType);
		haccpTest.setTesterName(user.getUserProfile().getName());

		if (material_id!=null) {
			if("P".equals(dataType)==false) {
				result.success = false;
				result.message = "잘못된 테스트구분값이 지정되었습니다.";
				return result;
			}
			
			Material material = this.materialRepository.getMaterialById(material_id);
			haccpTest.setMaterialId(material_id);
			haccpTest.setMaterialName(material.getName());
		}
		else {
			
			if("S".equals(dataType)) {
				haccpTest.setMaterialName("작업시작 전");
			}else {
				haccpTest.setMaterialName("작업종료 후");
			}
		}

		haccpTest.set_audit(user);
		this.haccpTestRepository.save(haccpTest);
		
		result.data = haccpTest.getId();

		return result;
	}
	
	
	@PostMapping("/delete_haccp_test")
	public AjaxResult deletHaccpTest(@RequestParam(value = "ht_id", required = true) int ht_id) {
		
		AjaxResult result = new AjaxResult();
		this.haccpDiaryService.deleteHaccpTestById(ht_id);
		//this.haccpTestRepository.deleteById(ht_id);
		
		return result;
	}

	@PostMapping("/save_haccp_test_item_result")
	public AjaxResult saveHaccpTestItemResult(
			@RequestParam(value = "Q", required = true) String q,
			HttpServletRequest request,
			Authentication auth	) {
		User user = (User)auth.getPrincipal();
		AjaxResult result = new AjaxResult();
		List<Map<String,Object>> test_items = CommonUtil.loadJsonListMap(q);
		
		try {
			this.haccpDiaryService.saveHaccpTestItemResult(test_items, user);
		} catch (Exception e) {
			result.success = false;
			result.message = e.toString();
		}

		return result;
	}	
	
	
	@GetMapping("/test_item_result_list")
	public AjaxResult getTestItemResultList(@RequestParam(value = "hd_id", required = true) int hd_id) {
		AjaxResult result = new AjaxResult();
		result.data = this.haccpDiaryService.getHaccpTestResultItemTreeList(hd_id);
		return result;
	}
	

	
	/**
	 * @param hd_id
	 * @return
	 */
	@GetMapping("/haccp_devi_detect_list")
	public AjaxResult getForeignDetectList(@RequestParam(value = "hd_id", required = true) int hd_id) {
		AjaxResult result = new AjaxResult();
		result.data = this.haccpDiaryService.getHaccpDeviDetectList(hd_id);
		return result;
	}
	
	
	@PostMapping("/delete_haccp_devi_detect")
	public AjaxResult deleteHaccpDiaryDeviDetect(@RequestParam(value = "hddd_id", required = true) int hddd_id) {
		AjaxResult result = new AjaxResult();
		
		//this.haccpDiaryService
		this.haccpDiaryDeviationDetectRepository.deleteById(hddd_id);
		
		return result;
	}
	
	@GetMapping("/haccpDeviDetectActionDetail")
	public AjaxResult getHaccpDeviDetectActionList(
			@RequestParam(value = "hddd_id", required = true) int hddd_id,
			@RequestParam(value = "parent_code", required = true) String parentCode) {
		AjaxResult result = new AjaxResult();
		Map<String, Object> mapData = new HashMap<>();
		
		Map<String, Object> item = this.haccpDiaryService.getHaccpDeviDetectActionDetail(hddd_id);
		List<Map<String, Object>> items = this.haccpDiaryService.getHaccpDeviDetectActionCodeList(parentCode);
		
		mapData.put("detail", item);
		mapData.put("items", items); 

		result.data = mapData;
		return result;
	}
	
	@PostMapping("/saveHaccpDetectAction")
	public AjaxResult saveHaccpDetectAction(
			@RequestParam(value = "hddd_id", required = true) int hddd_id,
			@RequestParam(value = "AbnormalDetail", required = true) String abnormalDetail,
			@RequestParam(value = "ActionCode", required = true) String actionCode,
			@RequestParam(value = "ActionDetail", required = true) String actionDetail,
			@RequestParam(value = "ActorName", required = false) String actorName,
			@RequestParam(value = "description", required = false) String description,
			Authentication auth
			) {
		AjaxResult result = new AjaxResult();
		User user  = (User)auth.getPrincipal();
		
		HaccpDiaryDeviationDetect haccpDiaryDeviationDetect = this.haccpDiaryDeviationDetectRepository.getHaccpDiaryDeviationDetectById(hddd_id);
		haccpDiaryDeviationDetect.setAbnormalDetail(abnormalDetail);
		haccpDiaryDeviationDetect.setActionDetail(actionDetail);
		haccpDiaryDeviationDetect.setActionCode(actionCode);
		haccpDiaryDeviationDetect.setActorName(actorName);
		haccpDiaryDeviationDetect.setDescription(description);
		haccpDiaryDeviationDetect.set_audit(user);
		this.haccpDiaryDeviationDetectRepository.save(haccpDiaryDeviationDetect);
		result.data = haccpDiaryDeviationDetect.getId();
		
		
		return result;
	}
}