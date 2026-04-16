package mes.app.haccp;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


import mes.app.haccp.service.YearVerificationPlanService;
import mes.domain.entity.BundleHead;
import mes.domain.entity.RelationData;
import mes.domain.entity.User;
import mes.domain.entity.YearVerificationMonthPlan;
import mes.domain.entity.YearVerificationPlan;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BundleHeadRepository;
import mes.domain.repository.RelationDataRepository;
import mes.domain.repository.YearVerificationMonthPlanRepository;
import mes.domain.repository.YearVerificationPlanRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/haccp/year_verification_plan")
public class YearVerificationPlanController {

	
	@Autowired
	YearVerificationPlanRepository yearVerificationPlanRepository;
	
	@Autowired
	YearVerificationMonthPlanRepository yearVerificationMonthPlanRepository;
	
	@Autowired
	private YearVerificationPlanService yearVerificationPlanService;
	
	@Autowired
	BundleHeadRepository bundleHeadRepository;
	
	@Autowired
	RelationDataRepository relationDataRepository;
	
	@Autowired
	SqlRunner sqlRunner;

	
	@GetMapping("/read")
	private AjaxResult getEduResult(
			@RequestParam("start_date") String start_date,
			@RequestParam("end_date") String end_date) {
		
		List<Map<String, Object>> items = this.yearVerificationPlanService.getList(start_date, end_date);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	
	@GetMapping("/yearVerRead")
	private AjaxResult getYearVerRead(
			@RequestParam("data_year") String dataYear,
			@RequestParam("bhId") Integer bhId,
			@RequestParam(value="data_date", required=false) String dataDate,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();
		
		
		Map<String, Object> items = this.yearVerificationPlanService.YearVerificationPlan(dataYear,bhId,auth,dataDate);
		
		
		result.data = items;
		
		return result;
	}
	
	
	
	
//	@GetMapping("/detail")
//	private AjaxResult getEduResultDetail(
//			@RequestParam(value = "id", required = false) Integer id) {
//		
//		Map<String, Object> items = this.eduResultService.getEduResultDetail(id);
//		
//		AjaxResult result = new AjaxResult();
//		result.data = items;
//		
//		return result;
//	}
	
	@PostMapping("/savePlan")
	private AjaxResult saveYearVerPlan(
			@RequestBody MultiValueMap<String,Object> Q,
			@RequestParam("data_year") Integer dataYear,
			@RequestParam("data_date") String data_date,
			@RequestParam("paramBhid")Integer paramBhid,
			@RequestParam("srchStartDt")String srchStartDt,
			@RequestParam("title")String title,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
		List<Map<String, Object>> items = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		Timestamp checkDate = Timestamp.valueOf(srchStartDt+ " 00:00:00");
		
		BundleHead bh = new BundleHead();
		
		
		Integer bhId = 0;
		if(paramBhid == 0) {
			bh.setTableName("year_verification_plan");
			bh.setChar1(title);
			bh.set_audit(user);
			bh.setDate1(checkDate);
			
			bh = this.bundleHeadRepository.save(bh);
			bhId = bh.getId();
		}else {
			
			MapSqlParameterSource paramMap = new MapSqlParameterSource();
			paramMap.addValue("paramBhid", paramBhid);
			
			String sql = """
					delete from rela_data where 1=1 AND "DataPk1" = :paramBhid AND "TableName1" = 'bundle_head' AND "TableName2" = 'year_verification_plan'
					""";
			this.sqlRunner.execute(sql, paramMap);
			
			sql ="""
					with T1 as (select yp.* from year_verification_plan yp
						left join rela_data rd on rd."DataPk2" = yp.id
						where 1=1
						AND rd."TableName2" = 'year_verification_plan'
						AND rd."DataPk1" = :paramBhid)
						delete from year_verification_month_plan where "YearVerPlanTargetId" = T1.id
					""";
			this.sqlRunner.execute(sql, paramMap);
			
			 sql = """
					delete from year_verification_plan yp
						left join rela_data rd on rd."DataPk2" = yp.id
						where 1=1
						AND rd."TableName2" = 'year_verification_plan'
						AND rd."DataPk1" = :paramBhid
					
					""";
			this.sqlRunner.execute(sql, paramMap);
			
			bhId = paramBhid;
		}
		
		Integer index = 0;
	    for (int i = 0; i< items.size(); i++) {
	    	index += 1; 
	    	Integer verYearTargetId = items.get(i).get("year_verification_plan_id") != null ? Integer.parseInt(items.get(i).get("year_verification_plan_id").toString()) : null;
	    	String verTarget = items.get(i).get("verification_target") != null ? items.get(i).get("verification_target").toString() : "";
	    	String verMethod = items.get(i).get("verification_method") != null ? items.get(i).get("verification_method").toString() : "";
		
	    	YearVerificationPlan yvp = null;
	    	
	    	if(verYearTargetId != null) {
	    		yvp = this.yearVerificationPlanRepository.getYearVerPlanById(verYearTargetId);
	    	} else {
	    		yvp = new YearVerificationPlan();
	    	}
	    	
	    	yvp.setDataYear(dataYear);
	    	yvp.setVerificationTarget(verTarget);
	    	yvp.setVerificationMethod(verMethod);
	    	yvp.set_order(index);
	    	yvp.set_audit(user);
	    	yvp = this.yearVerificationPlanRepository.save(yvp);
	    	
	    	
	    	RelationData rd = new RelationData();
	    	rd.setDataPk1(bhId);
	    	rd.setTableName1("bundle_head");
	    	rd.setDataPk2(yvp.getId());
	    	rd.setTableName2("year_verification_plan");
	    	rd.setRelationName("year_verification_plan");
	    	rd.set_audit(user);
	    	
	    	rd = this.relationDataRepository.save(rd);
	    	
	    	
	    	Integer id = yvp.getId();
	    	for (int j = 1; j < 13; j++) {
	    		
	    		List<YearVerificationMonthPlan> erList = this.yearVerificationMonthPlanRepository.findByYearVerPlanTargetIdAndDataMonth(id,j);
	    		
	    		YearVerificationMonthPlan yvfm = null;
	    		
	    		if(erList.size() > 0) {
	    			yvfm = erList.get(0);
	    		} else {
	    			yvfm = new YearVerificationMonthPlan();
	    			yvfm.setYearVerPlanTargetId(id);
	    			yvfm.setDataMonth(j);
	    		}
	    		String planYn = "";
	    		if (items.get(i).get("plan_"+ j) != null) {
					if (items.get(i).get("plan_"+ j).toString().equals("Y") || items.get(i).get("plan_"+ j).toString().equals("true")) {
						planYn = "Y";
					}
	    		}
	    		yvfm.setPlanYN(planYn);
	    		yvfm.set_audit(user);
	    		yvfm = this.yearVerificationMonthPlanRepository.save(yvfm);
	    		
	    		
	    	}
	    }
	    
	    Map<String, Object> map = new HashMap<>();
	    map.put("id",bhId);
	    
	    result.data = map;
	    	
	    return result;
	  }

	
	@PostMapping("/deletePlan")
	public AjaxResult deleteYearPlan(
			@RequestParam(value = "year_verification_plan_id", required = false) Integer yearVerPlanTargetId) {
		
		AjaxResult result = new AjaxResult();
		
		this.yearVerificationPlanService.deletePlan(yearVerPlanTargetId);
		this.yearVerificationPlanRepository.deleteById(yearVerPlanTargetId);
		
		return result;
	}
	
	
	@PostMapping("/delete")
	public AjaxResult deleteYearList(
			@RequestParam(value = "id", required = false) Integer id) {
		
		AjaxResult result = new AjaxResult();
		
		this.yearVerificationPlanService.deleteList(id);
		
		return result;
	}
	
}
