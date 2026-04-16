package mes.app.haccp;

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

import mes.app.haccp.service.EduYearPlanService;
import mes.domain.entity.BundleHead;
import mes.domain.entity.EduYearMonthPlan;
import mes.domain.entity.EduYearTarget;
import mes.domain.entity.RelationData;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BundleHeadRepository;
import mes.domain.repository.EduYearMonthPlanRepository;
import mes.domain.repository.EduYearTargetRepository;
import mes.domain.repository.RelationDataRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/haccp/edu_year_plan")
public class EduYearPlanController {

	@Autowired
	private EduYearPlanService eduYearPlanService;
	
	@Autowired
	EduYearTargetRepository eduYearTargetRepository;
	
	@Autowired
	EduYearMonthPlanRepository eduYearMonthPlanRepository;
	
	@Autowired
	BundleHeadRepository bundleHeadRepository;
	
	@Autowired
	RelationDataRepository relationDataRepository;
	
	@Autowired
	SqlRunner sqlRunner;
	
	// 결재현황 조회
	@GetMapping("/appr_stat")
	public AjaxResult getEduYearPlanApprStatus(
			@RequestParam(value="start_date", required=false) String startDate, 
			@RequestParam(value="end_date", required=false) String endDate, 
			@RequestParam(value="appr_state", required=false) String apprState, 
			HttpServletRequest request) {
		
        List<Map<String, Object>> items = this.eduYearPlanService.getEduYearPlanApprStatus(startDate,endDate,apprState);     
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	// 교육계획 조회
	@GetMapping("/read")
	public AjaxResult getEduYearPlanList(
    		@RequestParam(value="bh_id", required=false) Integer bh_id, 
			HttpServletRequest request) {
		
        Map<String, Object> items = this.eduYearPlanService.getEduYearPlanList(bh_id);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
		
//	@GetMapping("/read")
//	public AjaxResult getEduYearPlan(
//			@RequestParam("data_year") String dataYear) {
//		
//		List<Map<String, Object>> items = this.eduYearPlanService.getEduYearPlan(dataYear);
//		
//		AjaxResult result = new AjaxResult();
//		result.data = items;
//		
//		return result;
//	}
	
	//최초 등록
	@PostMapping("/insert")
	@Transactional
	public AjaxResult insertVerifiCheckPrereqList(
			@RequestParam(value="bh_id", required=false, defaultValue= "0") Integer bh_id,
			@RequestParam(value="title", required=false) String title,
			@RequestParam(value="data_date", required=false) String data_date,
			@RequestParam(value="data_year", required=false) Integer data_year,
			@RequestParam MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
				
		BundleHead bh = new BundleHead();
		bh.setTableName("edu_year_target");
		bh.setChar1(title);
		bh.setDate1(CommonUtil.tryTimestamp(data_date));
		bh.setText1(CommonUtil.tryString(data_year));
		bh.set_audit(user);
		bh = this.bundleHeadRepository.save(bh);
			
		List<Map<String, Object>> qItems = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		if (qItems.size() <= 0) {
			result.success = false;
			return result;
		}
		
		for(int i = 0; i < qItems.size(); i++) {
			//Integer eduYearTargetId = CommonUtil.tryInt(CommonUtil.tryString(qItems.get(i).get("edu_year_target_id")));
	    	String eduTitle = CommonUtil.tryString(qItems.get(i).get("edu_title"));
	    	String eduTarget = CommonUtil.tryString(qItems.get(i).get("edu_target"));
	    	String eduContent = CommonUtil.tryString(qItems.get(i).get("edu_content"));
	    	String remark = CommonUtil.tryString(qItems.get(i).get("remark"));
	    	
	    	EduYearTarget eyt = new EduYearTarget();
	    	eyt.setDataYear(data_year);
	    	eyt.setEduTarget(eduTarget);
	    	eyt.setEduTitle(eduTitle);
	    	eyt.setEduContent(eduContent);	    	eyt.setRemark(remark);
	    	eyt.setDataPk(bh.getId());
	    	eyt.setTableName("bundle_head");
	    	eyt.set_order(i+1);
	    	eyt.set_audit(user);
	    	eyt = this.eduYearTargetRepository.save(eyt);
	    	
	    	RelationData rd = new RelationData();
	    	rd.setDataPk1(bh.getId());
	    	rd.setTableName1("bundle_head");
	    	rd.setDataPk2(eyt.getId());
	    	rd.setTableName2("edu_year_target");
	    	rd.setRelationName("edu_year_target");
	    	rd.set_audit(user);
	    	this.relationDataRepository.save(rd);
	    	
    		for (int j = 1; j < 13; j++) {
	    		
	    		List<EduYearMonthPlan> eympList = this.eduYearMonthPlanRepository.findByEduYearTargetIdAndDataMonth(eyt.getId(), j);
	    		
	    		EduYearMonthPlan eymp = null;
	    		
	    		if(eympList.size() > 0) {
	    			eymp = eympList.get(0);
	    		} else {
	    			eymp = new EduYearMonthPlan();
	    			eymp.setEduYearTargetId(eyt.getId());
	    			eymp.setDataMonth(j);
	    		}
	    		String planYn = "";
	    		if (qItems.get(i).get("plan_"+ j) != null) {
					if (qItems.get(i).get("plan_"+ j).toString().equals("Y") || qItems.get(i).get("plan_"+ j).toString().equals("true")) {
						planYn = "Y";
					}
	    		}
	    		eymp.setPlanYN(planYn);
	    		eymp.set_audit(user);
	    		eymp = this.eduYearMonthPlanRepository.save(eymp);
	    		
	    	}
	    }
		
		Map<String, Object> item = new HashMap<String, Object>();
		item.put("id", bh.getId());
		result.data = item;
		return result;
	}
	
	@PostMapping("/save")
	@Transactional
	public AjaxResult saveEduYearPlan(
			@RequestParam(value="bh_id", required=false) Integer bh_id,
			@RequestParam(value="title", required=false) String title,
			@RequestParam(value="data_date", required=false) String data_date,
			@RequestParam(value="data_year", required=false) Integer data_year,
			@RequestParam MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
				
		BundleHead bh = this.bundleHeadRepository.getBundleHeadById(bh_id);
			
		List<Map<String, Object>> qItems = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		if (qItems.size() <= 0) {
			result.success = false;
			return result;
		}
		
		for(int i = 0; i < qItems.size(); i++) {
			Integer eduYearTargetId = CommonUtil.tryInt(CommonUtil.tryString(qItems.get(i).get("edu_year_target_id")));
	    	String eduTitle = CommonUtil.tryString(qItems.get(i).get("edu_title"));
	    	String eduTarget = CommonUtil.tryString(qItems.get(i).get("edu_target"));
	    	String eduContent = CommonUtil.tryString(qItems.get(i).get("edu_content"));
	    	String remark = CommonUtil.tryString(qItems.get(i).get("remark"));
	    	
	    	List<EduYearTarget> eytList = this.eduYearTargetRepository.findEduYearTargetById(eduYearTargetId);
	    	EduYearTarget eyt = null;
	    	if(eytList.size() > 0) {
	    		eyt = eytList.get(0);
    		} else {
    			eyt = new EduYearTarget();
    		}
	    	eyt.setDataYear(data_year);
	    	eyt.setEduTarget(eduTarget);
	    	eyt.setEduTitle(eduTitle);
	    	eyt.setEduContent(eduContent);
	    	eyt.setRemark(remark);
	    	eyt.setDataPk(bh.getId());
	    	eyt.setTableName("bundle_head");
	    	eyt.set_order(i+1);
	    	eyt.set_audit(user);
	    	eyt = this.eduYearTargetRepository.save(eyt);
	    	
	    	List<RelationData> rdList = this.relationDataRepository.findByDataPk1AndTableName1AndDataPk2AndTableName2(bh.getId(),"bundle_head", eyt.getId(),"edu_year_target");
	    	RelationData rd = null;
	    	if(rdList.size() > 0) {
	    		rd = rdList.get(0);
    		} else {
    			rd = new RelationData();
    		}
	    	rd.setDataPk1(bh.getId());
	    	rd.setTableName1("bundle_head");
	    	rd.setDataPk2(eyt.getId());
	    	rd.setTableName2("edu_year_target");
	    	rd.setRelationName("edu_year_target");
	    	rd.set_audit(user);
	    	this.relationDataRepository.save(rd);
	    	
    		for (int j = 1; j < 13; j++) {
	    		
	    		List<EduYearMonthPlan> eympList = this.eduYearMonthPlanRepository.findByEduYearTargetIdAndDataMonth(eyt.getId(), j);
	    		
	    		EduYearMonthPlan eymp = null;
	    		
	    		if(eympList.size() > 0) {
	    			eymp = eympList.get(0);
	    		} else {
	    			eymp = new EduYearMonthPlan();
	    			eymp.setEduYearTargetId(eyt.getId());
	    			eymp.setDataMonth(j);
	    		}
	    		String planYn = "";
	    		if (qItems.get(i).get("plan_"+ j) != null) {
					if (qItems.get(i).get("plan_"+ j).toString().equals("Y") || qItems.get(i).get("plan_"+ j).toString().equals("true")) {
						planYn = "Y";
					}
	    		}
	    		eymp.setPlanYN(planYn);
	    		eymp.set_audit(user);
	    		eymp = this.eduYearMonthPlanRepository.save(eymp);
	    		
	    		result.data = eymp;
	    	}
	    }
		
		Map<String, Object> item = new HashMap<String, Object>();
		item.put("id", bh.getId());
		result.data = item;
		return result;
	}
	
	@PostMapping("/delete")
	@Transactional
	public AjaxResult deleteEduYearPlan(
			@RequestParam(value="bh_id", required=false) Integer bh_id) {
		
		AjaxResult result = new AjaxResult();
		
		List<EduYearTarget> eytList = this.eduYearTargetRepository.findEduYearTargetByDataPk(bh_id);
		
		if(eytList.size() > 0) {
	        for(int i = 0; i < eytList.size(); i++) {
	        	Integer eduYearTargetId = eytList.get(i).getId();
	        	this.eduYearMonthPlanRepository.deleteByEduYearTargetId(eduYearTargetId);
	    		this.eduYearTargetRepository.deleteById(eduYearTargetId);
	        }
		}
		
		this.relationDataRepository.deleteByDataPk1AndTableName1AndTableName2(bh_id, "bundle_head", "edu_year_target");
		this.bundleHeadRepository.deleteById(bh_id);
		
		return result;
		
	}
}
