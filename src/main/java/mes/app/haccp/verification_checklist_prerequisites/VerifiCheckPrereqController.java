package mes.app.haccp.verification_checklist_prerequisites;

import java.sql.Date;
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
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.haccp.verification_checklist_prerequisites.service.VerifiCheckPrereqService;
import mes.domain.entity.BundleHead;
import mes.domain.entity.CheckItemResult;
import mes.domain.entity.CheckResult;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BundleHeadRepository;
import mes.domain.repository.CheckItemResultRepository;
import mes.domain.repository.CheckMasterRepository;
import mes.domain.repository.CheckResultRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/haccp/verifi_check_prereq")
public class VerifiCheckPrereqController {
	
	@Autowired
	private VerifiCheckPrereqService verifiCheckPrereqService;
	
	@Autowired
	BundleHeadRepository bundleHeadRepository;
	
	@Autowired
	CheckResultRepository checkResultRepository;
	
	@Autowired
	CheckItemResultRepository checkItemResultRepository;
	
	@Autowired
	CheckMasterRepository checkMasterRepository;
	
	@Autowired
	SqlRunner sqlRunner;

	// 결재현황 조회
	@GetMapping("/appr_stat")
	public AjaxResult getVerifiCheckPrereqApprStatus(
			@RequestParam(value="start_date", required=false) String startDate, 
			@RequestParam(value="end_date", required=false) String endDate, 
			@RequestParam(value="appr_state", required=false) String apprState, 
			HttpServletRequest request) {
		
        List<Map<String, Object>> items = this.verifiCheckPrereqService.getVerifiCheckPrereqApprStatus(startDate,endDate,apprState);     
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	// 점검표 조회
	@GetMapping("/read")
	public AjaxResult getVerifiCheckPrereqList(
    		@RequestParam(value="bh_id", required=false) Integer bh_id, 
			HttpServletRequest request) {
		
        Map<String, Object> items = this.verifiCheckPrereqService.getVerifiCheckPrereqList(bh_id);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
		
	//최초 등록
	@PostMapping("/insert")
	@Transactional
	public AjaxResult insertVerifiCheckPrereqList(
			@RequestParam(value="bh_id", required=false, defaultValue= "0") Integer bh_id,
			@RequestParam(value="check_master_id", required=false) Integer check_master_id,
			@RequestParam(value="title", required=false) String title,
			@RequestParam(value="data_date", required=false) String data_date,
			@RequestParam MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
				
		BundleHead bh = new BundleHead();
		bh.setTableName("verification_checklist_prerequisites");
		bh.setChar1(title);
		bh.setDate1(CommonUtil.tryTimestamp(data_date));
		bh.setNumber1((float)check_master_id);
		bh.set_audit(user);
		bh = this.bundleHeadRepository.save(bh);
		Integer bhId = bh.getId();
		
		CheckResult cr = new CheckResult();
		cr.setCheckMasterId(check_master_id);
		cr.setCheckDate(Date.valueOf(data_date));    
		cr.setCheckerName(user.getUserProfile().getName());
		cr.setSourceDataPk(bhId);
		cr.set_audit(user);
		cr = this.checkResultRepository.save(cr);
		Integer check_result_id = cr.getId();
		
		List<Map<String, Object>> qItems = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		if (qItems.size() == 0) {
			result.success = false;
			return result;
		}
		
		for(int i = 0; i < qItems.size(); i++) {
			Integer check_item_id = Integer.parseInt(qItems.get(i).get("id").toString());
	    	String result1 = CommonUtil.tryString(qItems.get(i).get("result1"));
	    	String description = CommonUtil.tryString(qItems.get(i).get("result2"));
	    	
	    	CheckItemResult cir = new CheckItemResult();
	    	cir.setCheckResultId(check_result_id);
	    	cir.setCheckItemId(check_item_id);
	    	cir.setResult1(result1);
	    	cir.setResult2(description);
	        cir.set_audit(user);
	        this.checkItemResultRepository.save(cir);
	    }
		
		Map<String, Object> item = new HashMap<String, Object>();
		item.put("id", bhId);
		result.data = item;
		return result;
	}
	
	// 수정 저장
	@PostMapping("/save")
	@Transactional
	public AjaxResult saveVerifiCheckPrereqList(
			@RequestParam("bh_id") Integer bh_id,
			@RequestParam(value="check_master_id", required=false) Integer check_master_id,
			@RequestParam(value="title", required=false) String title,
			@RequestParam(value="data_date", required=false) String data_date,
			@RequestParam MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		BundleHead bh = this.bundleHeadRepository.getBundleHeadById(bh_id);
		Integer bhId = bh.getId();
		
		CheckResult cr = this.checkResultRepository.getCheckResultBySourceDataPk(bhId);
		Integer check_result_id = cr.getId();
		
		List<Map<String, Object>> qItems = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		if (qItems.size() == 0) {
			result.success = false;
			return result;
		}
		
		for(int i = 0; i < qItems.size(); i++) {
			Integer check_item_id = Integer.parseInt(qItems.get(i).get("id").toString());
	    	String result1 = qItems.get(i).get("result1").toString();
	    	String description = CommonUtil.tryString(qItems.get(i).get("result2"));
	    	
	    	CheckItemResult cir = this.checkItemResultRepository.findByCheckResultIdAndCheckItemId(check_result_id, check_item_id);
	    	cir.setResult1(result1);
	    	cir.setResult2(description);
	        cir.set_audit(user);
	        this.checkItemResultRepository.save(cir);
	    }
		
		Map<String, Object> item = new HashMap<String, Object>();
		item.put("id", bhId);
		result.data = item;
		return result;
	}
	
	@PostMapping("/delete")
	public AjaxResult deleteVerifiCheckPrereqList(
			@RequestParam(value="bh_id", required=false, defaultValue= "0") Integer bhId,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();
		
		this.verifiCheckPrereqService.deleteVerifiCheckPrereqList(bhId);
		
		result.success = true;
		
		return result;
	}
}
