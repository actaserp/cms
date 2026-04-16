package mes.app.precedence;

import java.sql.Date;
import java.sql.Time;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.common.service.DeviActionService;
import mes.app.precedence.service.IllumZoneResultService;
import mes.domain.entity.BundleHead;
import mes.domain.entity.DeviationAction;
import mes.domain.entity.MasterResult;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BundleHeadRepository;
import mes.domain.repository.DeviationActionRepository;
import mes.domain.repository.MasterResultRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.DateUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/precedence/illum_zone_result")
public class IllumZoneResultContoller {
	
	@Autowired
	SqlRunner sqlRunner;
	
	@Autowired
	private DeviationActionRepository deviationActionRepository;
	
	@Autowired
	private DeviActionService deviActionService;
	
	@Autowired
	private BundleHeadRepository bundleHeadRepository;
	
	@Autowired
	private IllumZoneResultService illumZoneResultService;

	@Autowired
	MasterResultRepository masterResultRepository;
	
	// 관리구역 리스트. 당월 조도값과 이탈내역, 조치사항을 표시
	@GetMapping("/read")
	public AjaxResult getIllumResultList(
    		@RequestParam("bh_id") int bh_id,
    		@RequestParam(value="search_year", required=false) String search_year,
    		@RequestParam(value="search_month", required=false) String search_month,
    		@RequestParam(value="type2", required=false) String type2,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		Map<String, Object> items = this.illumZoneResultService.getIllumResultList(bh_id, search_year, search_month, type2, user);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}

	// 결과입력탭) 상세정보 조회  
	@GetMapping("/detail_master")
	public AjaxResult getMasterDetailList(
    		@RequestParam(value="master_id", required=false) Integer master_id,
    		@RequestParam(value="id", required=false) Integer id,
			HttpServletRequest request) {
			
		Map<String, Object> items = this.illumZoneResultService.getMasterDetailList(master_id, id);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	// 조도검사 결과입력탭) 저장
	@PostMapping("/save")
	public AjaxResult saveIllumResult(
			@RequestParam("bh_id") Integer bh_id,
			@RequestParam(value="title", required=false) String title,
			@RequestParam(value="year", required=false) String year,
			@RequestParam(value="month", required=false) String month,
			@RequestParam(value="type2", required=false) String type2,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
       
        AjaxResult result = new AjaxResult();
        
		User user = (User)auth.getPrincipal();

		String data_from = DateUtil.getFirstDayOfMonthByTodayString(); //yyyy-MM-dd
		String date_to = DateUtil.getLastDayOfMonthByTodayString(); //yyyy-MM-dd
		
		String dataDate = DateUtil.getTodayString(); // yyyy-MM-dd
		String dataTime = DateUtil.getHHmmByTodayString(); //hh:mm
		
		BundleHead bh = new BundleHead();
		if (bh_id > 0) {
			bh = this.bundleHeadRepository.getBundleHeadById(bh_id);
		}
		
		bh.setTableName("illum_zone");
		bh.setChar1(title);
		bh.setChar2(year);
		bh.setChar3(month);
		bh.setDate1(CommonUtil.tryTimestamp(dataDate));
		bh.setText1(type2);
		bh.set_audit(user);
		bh = this.bundleHeadRepository.save(bh);

		List<Map<String, Object>> qItems = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		if (qItems.isEmpty()) {
			result.success = false;
			return result;
		}
		for (int i = 0; i < qItems.size(); i++) {
			Integer id = CommonUtil.tryIntNull(qItems.get(i).get("id"));
			Integer master_id = CommonUtil.tryIntNull(qItems.get(i).get("master_id"));
			Integer qResult = CommonUtil.tryIntNull(qItems.get(i).get("result"));
	    	
			List<MasterResult> mrList = this.masterResultRepository.findByIdAndDataDateBetween(id,Date.valueOf(data_from),Date.valueOf(date_to));
			MasterResult mr;
			
	    	if(mrList.isEmpty()) {
	    		mr = new MasterResult();
	    		mr.setMasterTableId(master_id);
	    	}else {
	    		mr=mrList.get(0);
	    	}
	    	mr.setMasterClass("illum_zone");
	    	mr.setDataDate(Date.valueOf(dataDate));
	    	mr.setDataTime(Time.valueOf(dataTime+":00"));
	    	mr.setNumber1(qResult);
	    	mr.setSourceDataPk(bh.getId());
	    	mr.setSourceTableName("bundle_head");
	    	mr.setChar1(year);
	    	mr.setChar2(month);
	    	mr.setChar3(type2);
	    	mr.set_audit(user);
	    	mr = this.masterResultRepository.save(mr);
	    	id = mr.getId();
	    	
	    	Float standard = CommonUtil.tryFloatNull(qItems.get(i).get("standard"));
	    	
	    	if(qResult!=null && standard!=null && qResult < standard) {
	    		String abnormal_detail = CommonUtil.tryString(qItems.get(i).get("abnormal_detail"));
	    		String happen_place = "조도관리";
	    		String action_detail = "";
	            String confirm_detail = "";
	    		
	    		List<DeviationAction> daList = this.deviationActionRepository.findBySourceTableNameAndSourceDataPk("illum_zone", id);

	            if (daList.isEmpty()) {
	                deviActionService.saveDeviAction(0, id, "illum_zone", dataDate, happen_place,abnormal_detail, action_detail, confirm_detail, user);
	            }
	        } else {
	            List<DeviationAction> daList = this.deviationActionRepository.findBySourceTableNameAndSourceDataPk("illum_zone", id);

	            if (!daList.isEmpty()) {
	                Integer devi_id = daList.get(0).getId();
	                this.deviActionService.deleteDeviAction(devi_id);
	            }
	        }
	    }
		Map<String,Object> item = new HashMap<String,Object>();
		item.put("id", bh.getId());
	    result.success=true;
	    result.data=item;
		
		return result;
	}
	
	// 일지 삭제
	@PostMapping("/delete")
	@Transactional
	public AjaxResult deleteIllumResult(
			@RequestParam("bh_id") Integer bh_id,
			@RequestBody MultiValueMap<String,Object> Q) {
		
		AjaxResult result = new AjaxResult();
		
		List<Map<String, Object>> qItems = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		if (qItems.size() <= 0) {
			result.success = false;
			return result;
		}
		
		for (int i = 0; i < qItems.size(); i++) {
	    	Integer id = CommonUtil.tryIntNull(qItems.get(i).get("id"));
	    	
	    	this.deviationActionRepository.deleteBySourceDataPk(id);
	    	this.masterResultRepository.deleteById(id);
	    }
		this.bundleHeadRepository.deleteById(bh_id);
		result.success = true;
		
		return result;
	}

	// 측정결과탭) 관리구역 리스트. 최종 조도값과 최종측정일을 표시 
	@GetMapping("/read_result")
	public AjaxResult getMasterResultList(
    		@RequestParam(value="master_id", required=false) Integer master_id,
    		@RequestParam(value="data_year", required=false) String data_year,
			HttpServletRequest request) {

        List<Map<String, Object>> items = this.illumZoneResultService.getMasterResultList(master_id, data_year);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}	
	
	// 관리구역) 관리구역 리스트 조회
	@GetMapping("/read_master")
	public AjaxResult getMasterList(
    		@RequestParam(value="master_class", required=false) String master_class,
    		@RequestParam(value="base_date", required=false) String base_date,
			HttpServletRequest request) {

        List<Map<String, Object>> items = this.illumZoneResultService.getMasterList(master_class, base_date);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	// 결과입력탭) 저장
	@PostMapping("/save_result")
	public AjaxResult saveResult(
			@RequestParam(value="master_class", required=false) String master_class,
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam(value="master_id", required=false) Integer master_id,
			@RequestParam(value="DataDate", required=false) String DataDate,
			@RequestParam(value="DataTime", required=false) String DataTime,
			@RequestParam(value="Number1", required=false) Integer Number1,
			@RequestParam(value="Number2", required=false) Integer Number2,
			@RequestParam(value="Description", required=false) String Description,
			HttpServletRequest request,
			Authentication auth) {
       
        AjaxResult result = new AjaxResult();
        
		User user = (User)auth.getPrincipal();
		
		MasterResult mr = null;
		
		if (id == null) {
			mr = new MasterResult();		
		} else {
			mr = this.masterResultRepository.getMasterResultById(id);
		}


        mr.setMasterClass(master_class);
        mr.setMasterTableId(master_id);
        mr.setDataDate(Date.valueOf(DataDate));
        if (DataTime.length() == 5) {
            mr.setDataTime(Time.valueOf(DataTime + ":00"));
        } else {
            mr.setDataTime(Time.valueOf(DataTime));
        }
        mr.setNumber1(Number1);
        mr.setNumber2(Number2);
        mr.setDescription(Description);
        mr.set_audit(user);
        
        mr = this.masterResultRepository.save(mr);
		
        result.data = mr;
		return result;
	}
	
	// 측정결과탭) 삭제
	@PostMapping("/delete_result")
	public AjaxResult deleteResult(@RequestParam("id") Integer id) {
		
		if (id != null) {
			this.masterResultRepository.deleteById(id);
		}
		
		AjaxResult result = new AjaxResult();
		return result;
	}
	
}
