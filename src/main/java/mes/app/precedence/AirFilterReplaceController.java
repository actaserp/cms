package mes.app.precedence;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.common.service.FileService;
import mes.app.precedence.service.AirFilterReplaceService;
import mes.domain.entity.AttachFile;
import mes.domain.entity.MasterT;
import mes.domain.entity.MasterYearMonthPlan;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.AttachFileRepository;
import mes.domain.repository.MasterTRepository;
import mes.domain.repository.MasterYearMonthPlanRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/precedence/air_filter_replace")
public class AirFilterReplaceController {
	
	@Autowired
	private AirFilterReplaceService airFilterReplaceService;
	
	@Autowired
	MasterYearMonthPlanRepository masterYearMonthPlanRepository;
	
	@Autowired
	MasterTRepository masterTRepository;
	
	@Autowired
	AttachFileRepository attachFileRepository;
	
	@Autowired
	FileService fileService;
	
	@GetMapping("/year_month_plan_result_sheet")
	public AjaxResult yearMonthPlanResultSheet(
			@RequestParam(value="data_year") String dataYear,
			@RequestParam(value="period") String period
			) {
		
	    List<Map<String, Object>> items = this.airFilterReplaceService.yearMonthPlanResultSheet(dataYear,period);      
	    AjaxResult result = new AjaxResult();
	    result.data = items;        
		return result;
	}
	
	@GetMapping("/year_month_plan_result_list")
	public AjaxResult yearMonthPlanResultList(
			@RequestParam(value="data_year") String dataYear) {
		
	    List<Map<String, Object>> items = this.airFilterReplaceService.yearMonthPlanResultList(dataYear);      
	    AjaxResult result = new AjaxResult();
	    result.data = items;        
		return result;
	}
	
	@GetMapping("/detail")
	public AjaxResult getAirFilterDetail(
			@RequestParam("id") int id) {
		
		Map<String, Object> items = this.airFilterReplaceService.getAirFilterDetail(id);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/air_filter_info")
	public AjaxResult getAirFilterInfo(
			@RequestParam("master_id") int masterId) {
		
		MasterT mt = this.masterTRepository.findByIdAndMasterClass(masterId,"air_filter");
		
		Map<String, Object> items = new HashMap<>();
		
		if (mt != null) {
			items.put("master_id", mt.getId());
			items.put("Type2", mt.getType2());
			items.put("Description", mt.getDescription());
			items.put("Type", mt.getType());
		}
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@PostMapping("/save_year_plan")
	@Transactional
	public AjaxResult saveYearPlan(
			@RequestParam(value = "data_year", required = false) Integer dataYear,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
	    List<Map<String, Object>> items = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());

	    Integer index = 0;
		for (int i = 0; i < items.size(); i++) {
			index += 1;
			
			Integer masterId = Integer.parseInt(items.get(i).get("master_id").toString());
			
			MasterYearMonthPlan mp = null;
			for (int j = 1; j < 13; j++) {
				List<MasterYearMonthPlan> myList = this.masterYearMonthPlanRepository.findByDataYearAndDataMonthAndMasterTableId(dataYear,j,masterId); 
				if (myList.size() > 0) {
					mp = myList.get(0);
				} else {
					mp = new MasterYearMonthPlan();
					mp.setDataYear(dataYear);
					mp.setDataMonth(j);
					mp.setMasterTableId(masterId);
				}
				String planYn = "";
				if (items.get(i).get("p"+ j) != null) {
					if (items.get(i).get("p"+ j).toString().equals("Y") || items.get(i).get("p"+ j).toString().equals("true")) {
						planYn = "Y";
					}
				}
				mp.setPlanYN(planYn);
				mp.setMasterClass("");
				mp.set_audit(user);
				mp = this.masterYearMonthPlanRepository.save(mp);
				result.data = mp;
			}
			
			
		}
		return result;
	}
	
	@PostMapping("/save_month_result")
	public AjaxResult saveMonthResult(
			@RequestParam(value = "data_year", required = false) Integer dataYear,
			@RequestParam(value = "data_month", required = false) Integer dataMonth,
			@RequestParam(value = "master_id", required = false) Integer masterId,
			@RequestParam(value = "result", required = false) String resultYN,
			@RequestParam(value = "file_id1", required = false) String fileId1,
			@RequestParam(value = "file_id2", required = false) String fileId2,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult res = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
		MasterYearMonthPlan mp = null;
		List<MasterYearMonthPlan> myList = this.masterYearMonthPlanRepository.findByDataYearAndDataMonthAndMasterTableId(dataYear,dataMonth,masterId); 
		if (myList.size() > 0) {
			mp = myList.get(0);
		} else {
			mp = new MasterYearMonthPlan();
			mp.setDataYear(dataYear);
			mp.setDataMonth(dataMonth);
			mp.setMasterTableId(masterId);
		}
		mp.setResultYN(resultYN);
		mp.set_audit(user);
		mp = this.masterYearMonthPlanRepository.save(mp);
		Integer mpId = mp.getId();
		
		if (resultYN.equals("N")) {
			if (StringUtils.hasText(fileId1)) {
				List<AttachFile> af = this.attachFileRepository.findByIdAndAttachName(fileId1,"replace_filter1");
				
				for(int i = 0; i < af.size(); i++) {
					this.attachFileRepository.deleteById(af.get(i).getId());
				}
			}
			
			if (StringUtils.hasText(fileId2)) {
				List<AttachFile> af = this.attachFileRepository.findByIdAndAttachName(fileId2,"replace_filter2");
				
				for(int i = 0; i < af.size(); i++) {
					this.attachFileRepository.deleteById(af.get(i).getId());
				}
			}
		}
		
		if(StringUtils.hasText(fileId1)) {
			String[] fileIdList = fileId1.split(",");
			
			for (int i = 0; i < fileIdList.length; i++) {
				fileService.updateDataPk(Integer.parseInt(fileIdList[i]), mpId);
			}
		}
		
		if(StringUtils.hasText(fileId2)) {
			String[] fileIdList = fileId2.split(",");
			
			for (int i = 0; i < fileIdList.length; i++) {
				fileService.updateDataPk(Integer.parseInt(fileIdList[i]), mpId);
			}
		}
		res.data = mp;
		
		return res;
	}
	

}
