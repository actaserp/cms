package mes.app.precedence;

import java.sql.Date;
import java.sql.Timestamp;
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

import mes.app.precedence.service.UnsuitableProductService;
import mes.domain.entity.ActionData;
import mes.domain.entity.BundleHead;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.ActionDataRepository;
import mes.domain.repository.BundleHeadRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/precedence/unsuitable_product")
public class UnsuitableProductController {

	@Autowired
	private UnsuitableProductService unsuitableProductService;
	
	@Autowired
	BundleHeadRepository bundleHeadRepository;
	
	@Autowired
	ActionDataRepository actionDataRepository;
	
	// 결재현황 조회
	@GetMapping("/appr_stat")
	public AjaxResult getUnsuitableProductList(
			@RequestParam(value="start_date", required=false) String startDate, 
			@RequestParam(value="end_date", required=false) String endDate, 
			@RequestParam(value="appr_state", required=false) String apprState,
			@RequestParam(value="diary_type", required=false) String diaryType,
			HttpServletRequest request) {
		
        List<Map<String, Object>> items = this.unsuitableProductService.getUnsuitableProductList(startDate,endDate,apprState,diaryType);     
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@GetMapping("/defect_read")
	public AjaxResult defectDefect(
			@RequestParam(value="bh_id", required=false) Integer bh_id,
    		@RequestParam(value="diary_type", required=false) String diary_type,
    		@RequestParam(value="start_dt", required=false) String start_dt,
    		@RequestParam(value="end_dt", required=false) String end_dt,
    		@RequestParam(value="type", required=false) boolean type,
			HttpServletRequest request) {
		
        Map<String, Object> items = this.unsuitableProductService.defectDefect(bh_id, diary_type, start_dt, end_dt, type);      
        AjaxResult result = new AjaxResult();
        result.data = items;
		return result;
	}
	
	@PostMapping("/insert")
	@Transactional
	public AjaxResult insertUnsuitTable(
			@RequestParam(value="bh_id", required=false, defaultValue= "0") Integer bh_id,
			@RequestParam(value="title", required=false) String title,
			@RequestParam(value="data_date", required=false) String data_date,
			@RequestParam(value="start_dt", required=false) String startDt,
			@RequestParam(value="end_dt", required=false) String endDt,
			@RequestParam(value="diary_type", required=false) String diary_type,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();
		
		User user = (User)auth.getPrincipal();
		
		Timestamp today = new Timestamp(System.currentTimeMillis());
		
		BundleHead bh = new BundleHead();
		bh.setTableName("unsuitable_product_result");
		bh.setChar1(title);
		bh.setDate1(CommonUtil.tryTimestamp(data_date));
		bh.setText1(diary_type);
		bh.set_audit(user);
		
		bh = this.bundleHeadRepository.save(bh);
		Integer bhId = bh.getId();
		
		List<Map<String, Object>> items = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		String tableName = "";
		
		if (diary_type.equals("부적합품(입고)")) {
			tableName = "test_result";
		} else if(diary_type.equals("부적합품(생산)")){
			tableName = "job_res_defect";
		}
		for(int i = 0; i < items.size(); i++) {
			Integer id = CommonUtil.tryInt(CommonUtil.tryString(items.get(i).get("id")));
			String result1 = CommonUtil.tryString(items.get(i).get("result1"));
			String result2 = CommonUtil.tryString(items.get(i).get("result2"));
			String result3 = CommonUtil.tryString(items.get(i).get("result3"));
			String result4 = CommonUtil.tryString(items.get(i).get("result4"));
			String result5 = CommonUtil.tryString(items.get(i).get("result5"));
			
			ActionData ad = new ActionData();
			ad.setDataPk(id);
			ad.setTableName(tableName);
			ad.setCode(StringUtils.hasText(result1)  ? result1 : "");
			ad.setDescription(StringUtils.hasText(result2)  ? result2 : "");
			ad.setChar1(StringUtils.hasText(result3)  ? result3 : "");
			ad.setChar2(StringUtils.hasText(result4)  ? result4 : "");
			ad.setChar3(StringUtils.hasText(result5)  ? result5 : "");
			ad.setStartDate(Date.valueOf(startDt));
			ad.setEndDate(Date.valueOf(endDt));
			ad.setTableName2("bundle_head");
			ad.setDataPk2(bhId);
			ad.set_audit(user);
			ad.setActionDateTime(today);
			
			this.actionDataRepository.save(ad);
		}
		
	    result.data = bh.getId();
	    return result;
		
	}
	
	@PostMapping("/save")
	@Transactional
	public AjaxResult saveUnsuitTable(
			@RequestParam(value="bh_id", required=false, defaultValue= "0") Integer bh_id,
			@RequestParam(value="title", required=false) String title,
			@RequestParam(value="data_date", required=false) String data_date,
			@RequestParam(value="start_dt", required=false) String startDt,
			@RequestParam(value="end_dt", required=false) String endDt,
			@RequestParam(value="diary_type", required=false) String diary_type,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();
		
		User user = (User)auth.getPrincipal();
		
		Timestamp today = new Timestamp(System.currentTimeMillis());
		
		BundleHead bh = this.bundleHeadRepository.getBundleHeadById(bh_id);
		bh.setChar1(title);
		bh.setDate1(CommonUtil.tryTimestamp(data_date));
		bh.setText1(diary_type);
		bh.set_audit(user);
		
		bh = this.bundleHeadRepository.save(bh);
		
		Integer bhId = bh.getId();
		
		List<Map<String, Object>> items = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		String tableName = "";
		
		if (diary_type.equals("부적합품(입고)")) {
			tableName = "test_result";
		} else if(diary_type.equals("부적합품(생산)")){
			tableName = "job_res_defect";
		}
		
		List<ActionData> adList = this.actionDataRepository.findByDataPk2AndTableName2(bhId,"bundle_head");
		
		// 데이터 삭제
		for(int i = 0; i < adList.size(); i++) {
			this.actionDataRepository.deleteById(adList.get(i).getId());
		}
		
		for(int i = 0; i < items.size(); i++) {
			Integer id = CommonUtil.tryInt(CommonUtil.tryString(items.get(i).get("id")));
			String result1 = CommonUtil.tryString(items.get(i).get("result1"));
			String result2 = CommonUtil.tryString(items.get(i).get("result2"));
			String result3 = CommonUtil.tryString(items.get(i).get("result3"));
			String result4 = CommonUtil.tryString(items.get(i).get("result4"));
			String result5 = CommonUtil.tryString(items.get(i).get("result5"));
			
			ActionData ad = new ActionData();
			ad.setDataPk(id);
			ad.setTableName(tableName);
			ad.setCode(StringUtils.hasText(result1)  ? result1 : "");
			ad.setDescription(StringUtils.hasText(result2)  ? result2 : "");
			ad.setChar1(StringUtils.hasText(result3)  ? result3 : "");
			ad.setChar2(StringUtils.hasText(result4)  ? result4 : "");
			ad.setChar3(StringUtils.hasText(result5)  ? result5 : "");
			ad.setStartDate(Date.valueOf(startDt));
			ad.setEndDate(Date.valueOf(endDt));
			ad.setTableName2("bundle_head");
			ad.setDataPk2(bhId);
			ad.set_audit(user);
			ad.setActionDateTime(today);
			
			this.actionDataRepository.save(ad);
		}
		
	    result.data = bh.getId();
	    return result;
		
	}
	
	@PostMapping("/delete")
	@Transactional
	public AjaxResult deleteUnsuitable(
			@RequestParam(value="bh_id", required=false, defaultValue= "0") Integer bhId) {
		
		AjaxResult result = new AjaxResult();
		
		List<ActionData> adList = this.actionDataRepository.findByDataPk2AndTableName2(bhId, "bundle_head");
		
		for(int i = 0; i < adList.size(); i++) {
			this.actionDataRepository.deleteById(adList.get(i).getId());
		}
		
		this.bundleHeadRepository.deleteById(bhId);
		
		return result;
		
	}
	
}
