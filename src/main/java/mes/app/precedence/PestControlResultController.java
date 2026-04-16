package mes.app.precedence;

import java.sql.Date;
import java.sql.Timestamp;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.codehaus.jettison.json.JSONException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import mes.app.precedence.service.PestControlResultService;
import mes.domain.entity.BundleHead;
//import mes.domain.entity.CheckItem;
import mes.domain.entity.CheckMaster;
import mes.domain.entity.CheckResult;
import mes.domain.entity.DeviationAction;
import mes.domain.entity.Rela3Data;
import mes.domain.entity.RelationData;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BundleHeadRepository;
import mes.domain.repository.CheckItemRepository;
import mes.domain.repository.CheckMasterRepository;
import mes.domain.repository.CheckResultRepository;
import mes.domain.repository.DeviationActionRepository;
import mes.domain.repository.Rela3DataRepository;
import mes.domain.repository.RelationDataRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/precedence/pest_control_result")
public class PestControlResultController {
	
	@Autowired
	private PestControlResultService pestControlResultService;
	
	@Autowired
	BundleHeadRepository bundleHeadRepository;
		
	@Autowired
	Rela3DataRepository rela3DataRepository;
	
	@Autowired
	CheckMasterRepository checkMasterRepository;
	
	@Autowired
	CheckItemRepository checkItemRepository;
	
	@Autowired
	CheckResultRepository checkResultRepository;
	
	@Autowired
	RelationDataRepository relationDataRepository;
	
	@Autowired
	DeviationActionRepository deviationActionRepository;
	
	@Autowired
	SqlRunner sqlRunner;
	
	@GetMapping("/appr_stat")
	public AjaxResult getApprStat(
			@RequestParam(value="start_date", required=false) String startDate, 
			@RequestParam(value="end_date", required=false) String endDate, 
			@RequestParam(value="pest_trap_class", required=false) String pestTrapClass
			) {
		
        Map<String, Object> items = this.pestControlResultService.getApprStat(startDate, endDate, pestTrapClass);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
		
	}
	@GetMapping("/floor_list")
	public AjaxResult getFloorList(
			@RequestParam(value="pest_trap_class", required=false) String pestTrapClass
			) {
		
        List<Map<String, Object>> items = this.pestControlResultService.getFloorList(pestTrapClass);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@GetMapping("/trap_list")
	public AjaxResult getTrapList(
			@RequestParam(value="pest_trap_class", required=false) String pestTrapClass 
			) {
		
        List<Map<String, Object>> items = this.pestControlResultService.getTrapList(pestTrapClass);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
		
	}
	
	@GetMapping("/pest_list")
	public AjaxResult getPestList(
			@RequestParam(value="pest_trap_class", required=false) String pestTrapClass 
			) {
		
        List<Map<String, Object>> items = this.pestControlResultService.getPestList(pestTrapClass);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
		
	}
	
	@GetMapping("/read")
	public AjaxResult getPestControlResult(
			@RequestParam(value="bh_id", required=false) Integer bhId, 
			@RequestParam(value="data_date", required=false) String dataDate, 
			@RequestParam(value="pest_trap_class", required=false) String pestTrapClass,
			@RequestParam(value="pest_id_list", required=false) String pestIdList,
			Authentication auth) {
		
        Map<String, Object> items = this.pestControlResultService.getPestControlResult(bhId,dataDate,pestTrapClass,pestIdList,auth);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@GetMapping("/detail")
	public AjaxResult getPestControlResultDetail(
    		@RequestParam(value="id", required=false) Integer id,
			HttpServletRequest request) {
		
        Map<String, Object> items = this.pestControlResultService.getPestControlResultDetail(id);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@GetMapping("/pest_count_sheet")
	@ResponseBody
	public AjaxResult getPestCountSheet(@RequestParam(value="id", required=false) Integer id) {
        Map<String, Object> items = this.pestControlResultService.getPestCountSheet(id);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@SuppressWarnings("unchecked")
	@PostMapping("/save")
	@Transactional
	public AjaxResult savePestControlResult(
			@RequestParam(value="bh_id", required=false, defaultValue= "0") Integer bhId,
			@RequestParam(value="title", required=false) String title,
			@RequestParam(value="data_date", required=false) String data_date,
			@RequestParam(value="pest_trap_class", required=false) String pestTrapClass,
			@RequestParam MultiValueMap<String,Object> checkResult,
			HttpServletRequest request,
			Authentication auth) throws JSONException {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();

		BundleHead bh = new BundleHead();
		
		Timestamp dataDate = Timestamp.valueOf(data_date+ " 00:00:00");
		
		if (bhId > 0) {
			bh = this.bundleHeadRepository.getBundleHeadById(bhId);
		}
		
		bh.setTableName("check_result_pest_control_result");
		bh.setChar1(title);
		bh.setDate1(dataDate);
		bh.setChar2(pestTrapClass);
		
		bh.set_audit(user);
		
		this.bundleHeadRepository.save(bh);
		
		if (bhId > 0) {
			
			String sql = null;
			
			MapSqlParameterSource paramMap = new MapSqlParameterSource();
			paramMap.addValue("bhId", bhId);
			
			sql = """
					delete from devi_action
                    where "SourceTableName" = 'check_result'
                    and exists (
                        select id
                        from check_result 
                        where exists (
                            select 1 
                            from rela_data 
                            where "TableName1" = 'bundle_head' 
                            and "TableName2" = 'check_result' 
                            and "RelationName" = 'appr_stat' 
                            and "DataPk1" = :bhId 
                            and check_result.id = "DataPk2"
                        )
                    )
                    """;
			
			this.sqlRunner.execute(sql, paramMap);
	                    
	                    
			sql = """
                    delete from rela_data 
                    where "TableName1" = 'check_result' 
                    and "TableName2" = 'master_t' 
                    and "RelationName" = 'check_result-pest' 
                    and "DataPk1" in (
                        select id
                        from check_result 
                        where exists (
                            select 1 
                            from rela_data 
                            where "TableName1" = 'bundle_head' 
                            and "TableName2" = 'check_result' 
                            and "RelationName" = 'appr_stat' 
                            and "DataPk1" = :bhId 
                            and check_result.id = "DataPk2"
                        )
                    )
				 """;
			
			this.sqlRunner.execute(sql, paramMap);
					
			sql =	"""
                        delete from check_result 
                        where exists (
                            select 1 
                            from rela_data 
                            where "TableName1" = 'bundle_head' 
                            and "TableName2" = 'check_result' 
                            and "RelationName" = 'appr_stat' 
                            and "DataPk1" = :bhId 
                            and check_result.id = "DataPk2"
                        )                        
					""";
			
			this.sqlRunner.execute(sql, paramMap);
		}
		
		CheckMaster cm = this.checkMasterRepository.getByCode("방충방서점검");
		// CheckItem ci = this.checkItemRepository.findByCheckMasterIdAndCode(cm.getId(),"0001");
		
	    List<Map<String, Object>> items = CommonUtil.loadJsonListMap(checkResult.getFirst("check_results").toString());

		for(int i = 0; i < items.size(); i++) {
			CheckResult cr = new CheckResult();
			cr.setCheckDate(Date.valueOf(data_date));
			cr.setCheckerId(user.getId());
			cr.setCheckerName(user.getUserProfile().getName());
			cr.setCheckMasterId(cm.getId());
			cr.set_audit(user);
			
			if (items.get(i).containsKey("std_txt")) {
				if (items.get(i).get("std_txt") != null) {
					cr.setChar1(items.get(i).get("std_txt").toString());
				}
			}
			
			if (items.get(i).containsKey("devi_txt")) {
				if (items.get(i).get("devi_txt") != null) {
				cr.setChar2(items.get(i).get("devi_txt").toString());
				}
			}
			
			cr.setNumber1(Integer.parseInt(items.get(i).get("trap_id").toString()));
			
			if (items.get(i).containsKey("pest_total")) {
				if (items.get(i).get("pest_total") != null) {
				cr.setNumber2(Integer.parseInt(items.get(i).get("pest_total").toString()));
				}
			}
			
			this.checkResultRepository.save(cr);
			
			RelationData rd = new RelationData();
			rd.setTableName1("bundle_head");
			rd.setTableName2("check_result");
			rd.setRelationName("appr_stat");
			rd.setDataPk1(bh.getId());
			rd.setDataPk2(cr.getId());
			rd.set_audit(user);
			
			this.relationDataRepository.save(rd);
			
			if (cr.getChar1() != null && !cr.getChar1().equals("")) {
				DeviationAction da = new DeviationAction();
				da.setSourceDataPk(cr.getId());
				da.setSourceTableName("check_result");
				da.setHappenDate(dataDate);
				da.setAbnormalDetail(cr.getChar1());
				da.setConfirmDetail("");
				da.setActionDetail(cr.getChar2());
				da.setActionState("Y");
				da.setConfirmState("Y");
				da.set_audit(user);
				
				this.deviationActionRepository.save(da);
			}
			
			List<Map<String, Object>> items2 = (List<Map<String, Object>>) items.get(i).get("pest_results");
			
			for (int j = 0 ; j < items2.size(); j++) {
				
				RelationData crr = new RelationData();
				crr.setTableName1("check_result");
				crr.setTableName2("master_t");
				crr.setRelationName("check_result-pest");
				crr.setDataPk1(cr.getId());
				if (items2.get(j).get("pest_id") != null) {
					crr.setDataPk2(Integer.parseInt(items2.get(j).get("pest_id").toString()));
				}
				
				if (items2.get(j).get("result_val") != null) {
					crr.setChar1(items2.get(j).get("result_val").toString());
				}
				
				crr.set_order(Integer.parseInt(items2.get(j).get("order").toString()));
				crr.set_audit(user);
				
				this.relationDataRepository.save(crr);
			}
		}
		
		Map<String , Object> value = new HashMap<String, Object>();
		value.put("id", bh.getId());
		
		result.data = value;
		
		return result;
	}
	
	@PostMapping("/delete")
	@Transactional
	public AjaxResult deletePestControlResult(@RequestParam(value="bh_id", required=false) Integer bhId) {
		
		AjaxResult result = new AjaxResult();
		
		String sql = null;
		
		this.bundleHeadRepository.deleteById(bhId);
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		
		sql = """
				delete from devi_action
                where "SourceTableName" = 'check_result'
                and exists (
                    select id
                    from check_result 
                    where exists (
                        select 1 
                        from rela_data 
                        where "TableName1" = 'bundle_head' 
                        and "TableName2" = 'check_result' 
                        and "RelationName" = 'appr_stat' 
                        and "DataPk1" = :bhId 
                        and check_result.id = "DataPk2"
                    )
                )
                """;
		
		this.sqlRunner.execute(sql, paramMap);
                    
                    
		sql = """
                delete from rela_data 
                where "TableName1" = 'check_result' 
                and "TableName2" = 'master_t' 
                and "RelationName" = 'check_result-pest' 
                and "DataPk1" in (
                    select id
                    from check_result 
                    where exists (
                        select 1 
                        from rela_data 
                        where "TableName1" = 'bundle_head' 
                        and "TableName2" = 'check_result' 
                        and "RelationName" = 'appr_stat' 
                        and "DataPk1" = :bhId 
                        and check_result.id = "DataPk2"
                    )
                )
			 """;
		
		this.sqlRunner.execute(sql, paramMap);
				
		sql =	"""
                    delete from check_result 
                    where exists (
                        select 1 
                        from rela_data 
                        where "TableName1" = 'bundle_head' 
                        and "TableName2" = 'check_result' 
                        and "RelationName" = 'appr_stat' 
                        and "DataPk1" = :bhId 
                        and check_result.id = "DataPk2"
                    )                        
				""";
		
		this.sqlRunner.execute(sql, paramMap);
		
		return result;
	}
	
	@PostMapping("/save_pest_count")
	@Transactional
	public AjaxResult savePestCount(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam MultiValueMap<String,Object> Q,
			@RequestParam(value="pest_id_list", required=false) String pestIdList,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		String[] pestId = pestIdList.split(",");
		
		List<Map<String, Object>> data = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		List<Rela3Data> rd = this.rela3DataRepository.findByRelationNameAndDataPk1AndTableName1AndTableName2AndTableName3("pest_control_detail", id, "doc_result", "master_t","master_t");
		
		for (int i = 0; i < rd.size(); i++) {
			this.rela3DataRepository.deleteById(rd.get(i).getId());
		}
		
		this.rela3DataRepository.flush();
		
		for (int i = 0; i < data.size(); i++) {
			Integer trapId = Integer.parseInt(data.get(i).get("trap_id").toString());
			for (int j = 0; j < pestId.length; j++) {
				Integer pestCount = data.get(i).get("pest_" + (j+1)) != null  && data.get(i).get("pest_" + (j+1)) != "" ? Integer.parseInt(data.get(i).get("pest_" + (j+1)).toString()) : 0;
				if(pestCount > 0) {
					Rela3Data rd3 = new Rela3Data();
					rd3.set_audit(user);
					rd3.setRelationName("pest_control_detail");
					rd3.setTableName1("doc_result");
					rd3.setDataPk1(id);
					rd3.setTableName2("master_t");
					rd3.setDataPk2(trapId);
					rd3.setTableName3("master_t");
					rd3.setDataPk3(Integer.parseInt(pestId[j]));
					rd3.setNumber1((float)pestCount);
					rd3 = this.rela3DataRepository.save(rd3);
				}
			}
			
		}
		return result;
	}
}
