package mes.app.precedence;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.precedence.service.SanitationCheckResultService;
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
@RequestMapping("/api/precedence/sanitation_check_result")
public class SanitationCheckResultController {

	@Autowired
	private SanitationCheckResultService sanitationCheckResultService;
	
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
	
	@GetMapping("/read_common")
	public AjaxResult getSanitationCheck(
			@RequestParam(value="check_master_id", required=false) Integer checkMasterId, 
			@RequestParam(value="start_date", required=false) String startDate, 
			@RequestParam(value="end_date", required=false) String endDate, 
			@RequestParam(value="appr_status", required=false) String apprStatus, 
			@RequestParam(value="type", required=false) String type, 
			HttpServletRequest request) {
		
		List<Map<String,Object>> items = this.sanitationCheckResultService.getSanitationCheck(checkMasterId,startDate,endDate,apprStatus,type);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}
	
	
	
	@GetMapping("/readList")
	public AjaxResult getCheckItem(
			@RequestParam("check_master_id") Integer checkMasterId,
			@RequestParam(value = "check_date", required = false) String checkDate,
			@RequestParam(value = "start_date", required = false) String startDate,
			@RequestParam(value = "end_date", required = false) String endDate) 
	 {
		
        List<Map<String, Object>> items = this.sanitationCheckResultService.getCheckItem(checkMasterId, checkDate, startDate, endDate);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	
	
	
	@GetMapping("/result_list")
	public AjaxResult getResultList(
			@RequestParam(value="bh_id", required=false) Integer bhId,
			@RequestParam(value="check_master_id", required=false) Integer checkMasterId,
			HttpServletRequest request) {
		
		Map<String, Object> items = this.sanitationCheckResultService.getResultList(bhId, checkMasterId);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}
	
	@SuppressWarnings("unchecked")
	@PostMapping("/insert")
	@Transactional
	public AjaxResult insertSanitationCheck(
			@RequestParam(value="check_master_id", required=false) Integer checkMasterId,
			@RequestParam(value="title", required=false) String title,
			@RequestParam(value="check_date", required=false) String check_date,
			@RequestParam MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		Timestamp checkDate = Timestamp.valueOf(check_date+ " 00:00:00");
		
		BundleHead bh = new BundleHead();
		bh.setTableName("check_result_sanitation_check_result");
		bh.setChar1(title);
		bh.setDate1(checkDate);
		bh.setNumber1((float)checkMasterId);
		bh.set_audit(user);

		
		this.bundleHeadRepository.save(bh);
		
		Integer bhId = bh.getId();
		
		List<Map<String, Object>> items = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		for(int i = 0; i < items.size(); i++) {
			CheckResult cr = new CheckResult();
			cr.setSourceDataPk(bhId);
			cr.setCheckMasterId(checkMasterId);
			cr.setCheckDate(Date.valueOf(check_date));
			cr.setCheckerName(user.getUserProfile().getName());
			
			if (items.get(i).containsKey("description")) {
				if (items.get(i).get("description") != null) {
					cr.setDescription(items.get(i).get("description").toString());
				}
			}

			cr.set_audit(user);
			this.checkResultRepository.save(cr);
			Integer checkResultId = cr.getId();
			
			List<Map<String, Object>> items2 = (List<Map<String, Object>>) items.get(i).get("tabList");
			
			for (int j = 0; j < items2.size(); j++) {
				CheckItemResult cir = new CheckItemResult();
				cir.setCheckResultId(checkResultId);
				cir.setCheckItemId(Integer.parseInt(items2.get(j).get("id").toString()));
				
				if (items2.get(j).get("result1") != null) {
					cir.setResult1(items2.get(j).get("result1").toString());
				} else {
					cir.setResult1(null);
				}
				
				if (items2.get(j).get("index_order") != null) {
					cir.setOrder(Integer.parseInt(items2.get(j).get("index_order").toString()));
				}
                
				cir.set_audit(user);
				this.checkItemResultRepository.save(cir);
			}
		}
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		paramMap.addValue("checkMasterId", checkMasterId);
		paramMap.addValue("userId", user.getId());
		
		String sql = """
				insert into devi_action("SourceDataPk", "SourceTableName", "HappenDate", "HappenPlace"
	            , "AbnormalDetail", _created, _creater_id)
                select ir.id as src_data_pk, 'check_item_devi_result' , cast(to_char(b."Date1", 'yyyy-MM-dd') as date) as happen_date ,cm."Name" as happen_place
	                , concat('부적합 : ', coalesce(ci."ItemGroup1",' '), coalesce(ci."ItemGroup2", ' '), coalesce(ci."ItemGroup3",' '), ci."Name") as abnormal_detail       
                ,current_date, :userId
                from bundle_head b
                inner join check_result cr on b.id = cr."SourceDataPk"
                inner join check_item_result ir on cr.id = ir."CheckResult_id" and ir."Result1" = 'X'
                inner join check_item ci on ir."CheckItem_id" = ci.id
                inner join check_mast cm on cm.id = b."Number1"
                where cr."SourceDataPk" = :bhId
                and cr.id not in (
	            select a."SourceDataPk" 
	            from devi_action a
	            inner join check_item_result b on a."SourceDataPk" = b.id
	            inner join check_result cr on b."CheckResult_id" = cr.id and cr."SourceDataPk" = :bhId
	            where a."SourceTableName" = 'check_item_devi_result'
                )
                order by ir._order	
				""";
		
		this.sqlRunner.execute(sql, paramMap);
		
		Map<String, Object> value = new HashMap<String, Object>();
		value.put("id", bh.getId());
		
		result.data = value;
		
		return result;
	}
	
	@SuppressWarnings("unchecked")
	@PostMapping("/save")
	@Transactional
	public AjaxResult saveSanitationCheck(
			@RequestParam(value="bh_id", required=false, defaultValue= "0") Integer bhId,
			@RequestParam(value="title", required=false) String title,
			@RequestParam(value="check_master_id", required=false) Integer checkMasterId,
			@RequestParam(value="check_date", required=false) String check_date,
			@RequestParam MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		Timestamp checkDate = Timestamp.valueOf(check_date+ " 00:00:00");
		
		BundleHead bh = this.bundleHeadRepository.getBundleHeadById(bhId);
		bh.setChar1(title);
		bh.setDate1(checkDate);
		bh.set_audit(user);
		
		this.bundleHeadRepository.save(bh);
		
		List<Map<String, Object>> items = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		for(int i = 0; i < items.size(); i++) {
			
			CheckResult cr = this.checkResultRepository.getCheckResultById(Integer.parseInt(items.get(i).get("id").toString()));
			cr.setCheckerName(user.getUserProfile().getName());
			
			if (items.get(i).containsKey("description")) {
				if (items.get(i).get("description") != null) {
					cr.setDescription(items.get(i).get("description").toString());
				}
			}
			
			
			cr.set_audit(user);
			this.checkResultRepository.saveAndFlush(cr);
			
			List<Map<String, Object>> items2 = (List<Map<String, Object>>) items.get(i).get("item_result");
			
			for (int j = 0; j < items2.size(); j++) {
				CheckItemResult cir = this.checkItemResultRepository.getCheckItemResultById(Integer.parseInt(items2.get(j).get("id").toString()));
				
				if (items2.get(j).get("result1") != null) {
					cir.setResult1(items2.get(j).get("result1").toString());
				} else {
					cir.setResult1(null);
				}
                
				cir.set_audit(user);
				this.checkItemResultRepository.saveAndFlush(cir);
			}
		}
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		paramMap.addValue("checkMasterId", checkMasterId);
		paramMap.addValue("userId", user.getId());
		
		String sql = null;
		
		sql = """
                insert into devi_action("SourceDataPk", "SourceTableName", "HappenDate", "HappenPlace"
	            , "AbnormalDetail", _created, _creater_id)
                select ir.id as src_data_pk, 'check_item_devi_result' , cast(to_char(b."Date1", 'yyyy-MM-dd') as date) as happen_date ,cm."Name" as happen_place
	            , concat('부적합 : ', coalesce(ci."ItemGroup1",' '), coalesce(ci."ItemGroup2", ' '), coalesce(ci."ItemGroup3",' '), ci."Name") as abnormal_detail
                , current_date, :userId
                from bundle_head b
                inner join check_result cr on b.id = cr."SourceDataPk"
                inner join check_item_result ir on cr.id = ir."CheckResult_id" and ir."Result1" = 'X' 
                inner join check_item ci on ir."CheckItem_id" = ci.id
                inner join check_mast cm on cm.id = b."Number1"
                where cr."SourceDataPk" =  :bhId
                and ir.id not in (
	                select a."SourceDataPk"
	                from devi_action a
	                inner join check_item_result b on a."SourceDataPk" = b.id
	                inner join check_result cr on b."CheckResult_id" = cr.id and cr."SourceDataPk" = :bhId
	                where a."SourceTableName" = 'check_item_devi_result'	
                )
                order by ir._order
                

				""";
		
		this.sqlRunner.execute(sql, paramMap);
		
		sql = """
			    delete from devi_action
                where "SourceDataPk" not in (
	                select b.id
	                from check_result a
	                inner join check_item_result b on a.id = b."CheckResult_id" and b."Result1" = 'X'
	                left join  devi_action c on c."SourceDataPk" = b.id
	                where 1=1	
                )
                and "SourceTableName" = 'check_item_devi_result'
			  """;
		
		this.sqlRunner.execute(sql, paramMap);
		
		Map<String, Object> value = new HashMap<String, Object>();
		value.put("id", bhId);
		
		result.data = value;
		
		return result;
	}
	
	@PostMapping("/mst_delete")
	public AjaxResult mstDelete(
			@RequestParam(value="bh_id", required=false, defaultValue= "0") Integer bhId,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();
		
		this.sanitationCheckResultService.mstDelete(bhId);
		
		result.success = true;
		
		return result;
	}
}
