package mes.app.precedence;

import java.sql.Timestamp;
import java.sql.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.common.service.DeviActionService;
import mes.app.precedence.service.AirConfresiaService;
import mes.domain.entity.BundleHead;
import mes.domain.entity.CheckItemResult;
import mes.domain.entity.CheckResult;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BundleHeadRepository;
import mes.domain.repository.CheckItemResultRepository;
import mes.domain.repository.CheckResultRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/precedence/air_confresia")
public class AirConfresiaController {
	
	@Autowired
	AirConfresiaService airConfresiaService;
	
	@Autowired
	BundleHeadRepository bundleHeadRepository;
	
	@Autowired
	CheckResultRepository checkResultRepository;
	
	@Autowired
	CheckItemResultRepository checkItemResultRepository;
	
	@Autowired
	SqlRunner sqlRunner;
	
	@Autowired
	DeviActionService deviActionService;
	
	
	@GetMapping("/appr_stat")
	public AjaxResult getApprStat(    		
			@RequestParam(value="start_date", required=false) String startDate, 
    		@RequestParam(value="end_date", required=false) String endDate, 
    		@RequestParam(value="appr_state", required=false) String apprState,
			HttpServletRequest request) {

		Map<String, Object> items = this.airConfresiaService.getApprStat(startDate,endDate,apprState);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}
	
	
	
	@GetMapping("/read")
	public AjaxResult getApprStatRead(    		
			@RequestParam(value="bhId", required=false) Integer bhId, 
    		@RequestParam(value="data_date", required=false) String data_Date, 
    		Authentication auth,
			HttpServletRequest request) {

		Map<String, Object> items = this.airConfresiaService.getApprStatRead(bhId,data_Date,auth);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}
	
	
		
	//@SuppressWarnings("unchecked")
	@PostMapping("/save")
	public AjaxResult getApprStatSave(    		
			@RequestParam(value="bhId", required=false) Integer bhId, 
    		@RequestParam(value="data_date", required=false) String data_Date,
    		@RequestParam(value="check_step", required=false) Integer check_step,
    		@RequestParam(value="next_yn", required=false) String next_yn,
    		@RequestParam(value="resultDateLength", required=false) Integer resultDateLength,
    		@RequestParam (value="headInfo") String headInfo,
			@RequestParam (value="diaryInfo") String diaryInfo,
    		Authentication auth,
    		HttpServletRequest request
    		)throws JSONException {
			
		
		User user = (User)auth.getPrincipal();
		
		JSONObject headInfo1 = new JSONObject(headInfo);
		
		List<Map<String, Object>> diaryInfo1 = CommonUtil.loadJsonListMap(diaryInfo);
		
		AjaxResult result = new AjaxResult();
		
		Timestamp checkDate = Timestamp.valueOf(data_Date+ " 00:00:00");
		
		Integer checkResultId=0;
		
		BundleHead bh = new BundleHead();
		
		if(bhId > 0) {
			
			bh = this.bundleHeadRepository.getBundleHeadById(bhId);
			
			MapSqlParameterSource paramMap = new MapSqlParameterSource();
			paramMap.addValue("bhId", bhId);
			
			String sql = """
					 update appr_result set _status = 'T' where "SourceDataPk" = :bhId and "SourceTableName"tl = 'bundle_head'
					""";
			this.sqlRunner.execute(sql, paramMap);
			
		}else {
			bh.setDate1(checkDate);
			//bh.setNumber1((float)headInfo1.get("CheckMaster_id"));
			bh.setTableName("check_result_cross_contamination1");
		}
		bh.setChar1(headInfo1.get("Title").toString());
		bh.setChar2(check_step.toString());
		bh.set_audit(user);
		this.bundleHeadRepository.save(bh);
		
		Integer bh_id = bh.getId();
		
		//bhId = bh.getId();
		
		checkResultId = (Integer) headInfo1.get("check_result_id");
		for(int k=1; k<resultDateLength+1; k++) {
			if(bhId > 0) {
				CheckResult cr = this.checkResultRepository.getCheckResultById(checkResultId);
					cr.setCheckDate(Date.valueOf(diaryInfo1.get(k).get("resultDate"+k).toString()));
					cr.setNumber2(k);
					cr.setCheckerId(user.getId());
					cr.setCheckerName(user.getUserProfile().getName());
					cr.set_audit(user);
					this.checkResultRepository.save(cr);
					headInfo1.put("check_result_id",cr.getId());
					
					for(int i=0; i<diaryInfo1.size(); i++) {
						Integer check_result_id = (Integer)headInfo1.get("check_result_id");
						Integer itemId = (Integer)diaryInfo1.get(i).get("item_id");
							CheckItemResult cir = this.checkItemResultRepository.findByCheckResultIdAndCheckItemIdAndResult3(check_result_id,itemId,Integer.toString(k));
							if(cir != null) {
								cir.setCheckResultId((Integer)headInfo1.get("check_result_id"));
								cir.setCheckItemId((Integer)diaryInfo1.get(i).get("item_id"));
									if(diaryInfo1.get(i).get("result"+k) != null) {
										cir.setResult1(diaryInfo1.get(i).get("result"+k).toString());
									}else {
										cir.setResult1(null);
									}
									cir.setResult2(diaryInfo1.get(i).get("resultDate"+k).toString());
									cir.setResult3(String.valueOf(k));
									cir.set_created(checkDate);
									cir.set_audit(user);
									this.checkItemResultRepository.save(cir);
							}else {
								CheckItemResult cir1 = new CheckItemResult();
								cir1.setCheckResultId((Integer)headInfo1.get("check_result_id"));
								cir1.setCheckItemId((Integer)diaryInfo1.get(i).get("item_id"));
									if(diaryInfo1.get(i).get("result"+k) != null) {
										cir1.setResult1(diaryInfo1.get(i).get("result"+k).toString());
									}else {
										cir1.setResult1(null);
									}
									cir1.setResult2(diaryInfo1.get(i).get("resultDate"+k).toString());
									cir1.setResult3(String.valueOf(k));
									cir1.set_created(checkDate);
									cir1.set_audit(user);
									this.checkItemResultRepository.save(cir1);
							}
					}
			}else {
				CheckResult cr = new CheckResult();
					cr.setSourceDataPk(bh_id);
					cr.setSourceTableName("bundle_head");
					cr.setCheckMasterId(headInfo1.getInt("CheckMaster_id"));
					cr.setCheckDate(Date.valueOf(diaryInfo1.get(k).get("resultDate"+k).toString()));
					cr.setNumber2(k);
					cr.setCheckerId(user.getId());
					cr.setCheckerName(user.getUserProfile().getName());
					cr.set_audit(user);
					this.checkResultRepository.save(cr);
					headInfo1.put("check_result_id",cr.getId());
					
					for(int i=0; i<diaryInfo1.size(); i++) {
						CheckItemResult cir = new CheckItemResult();
							cir.setCheckResultId((Integer)headInfo1.get("check_result_id"));
							cir.setCheckItemId((Integer)diaryInfo1.get(i).get("item_id"));
							if(diaryInfo1.get(i).get("result"+k) != null) {
								cir.setResult1(diaryInfo1.get(i).get("result"+k).toString());
							}else {
								cir.setResult1(null);
							}
							cir.setResult2(diaryInfo1.get(i).get("resultDate"+k).toString());
							cir.setResult3(String.valueOf(k));
							cir.set_created(checkDate);
							cir.set_audit(user);
							this.checkItemResultRepository.save(cir);
					}
			}
		}
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bh_id);
		paramMap.addValue("check_result_id",checkResultId);
		paramMap.addValue("userId", user.getId());
		
		
		String sql = """
				   select cir.id as src_data_pk ,'devi_action_cross_contamination1' as source_table_name , cr."CheckDate" as happen_date, cm."Name" as happen_place
	            , concat('부적합 : ', coalesce(ci."ItemGroup1",' '), coalesce(ci."ItemGroup2", ' '), coalesce(ci."ItemGroup3",' '), ci."Name") as abnormal_detail
	            ,null as action_detail ,null as confirm_detail
	            from check_result cr
	            inner join check_item_result cir on cir."CheckResult_id" = cr.id
	            inner join check_item ci on ci.id = cir."CheckItem_id"
	            inner join check_mast cm on cm.id = cr."CheckMaster_id"	
	            where cr.id = :check_result_id
                and cir.id not in (
	            select a."SourceDataPk"
	            from devi_action a
                inner join check_item_result b on a."SourceDataPk" = b.id
                inner join check_result c on b."CheckResult_id" = c.id and c."SourceDataPk" = :bhId
	            where a."SourceTableName" = 'devi_action_cross_contamination1' )
                and cir."Result1" = 'X'
				""";
		
		List<Map<String, Object>> devi = this.sqlRunner.getRows(sql, paramMap);
		
		if(devi.size() > 0) {
			for(int i=0; i<devi.size(); i++) {
				Integer sourcePk = devi.get(i).get("src_data_pk") != null ? Integer.parseInt(devi.get(i).get("src_data_pk").toString()) : null;
        		String sourceTableName =  devi.get(i).get("source_table_name") != null ? devi.get(i).get("source_table_name").toString() : "";
        		String happenDate  =  devi.get(i).get("happen_date") != null ? devi.get(i).get("happen_date").toString() : "";
        		String happenPlace  =  devi.get(i).get("happen_place") != null ? devi.get(i).get("happen_place").toString() : "";
        		String abnormalDetail  =  devi.get(i).get("abnormal_detail") != null ? devi.get(i).get("abnormal_detail").toString() : "";
        		String actionDetail  =  devi.get(i).get("action_detail") != null ? devi.get(i).get("action_detail").toString() : "";
        		String confirmDetail  =  devi.get(i).get("confirm_detail") != null ? devi.get(i).get("confirm_detail").toString() : "";
        		
        		deviActionService.saveDeviAction(0, sourcePk,sourceTableName,happenDate,happenPlace, abnormalDetail,actionDetail,confirmDetail, user);
        	}	
			
		}
		
		sql = """
				select id from devi_action da
                    where da."SourceTableName" = 'devi_action_cross_contamination1'
                    and da."SourceDataPk" not in 
                    (
	                    select b.id
		                from check_result a
		                inner join check_item_result b on a.id = b."CheckResult_id" and b."Result1" = 'X'
		                left join  devi_action c on c."SourceDataPk" = b.id
		                where 1=1
	                    and b."Result1" = 'X'
                    )
        		""";
		List<Map<String, Object>> del_devi = this.sqlRunner.getRows(sql, paramMap);
		
		if (del_devi.size() > 0) {
        	for (int i = 0; i < del_devi.size(); i++) {
        		deviActionService.deleteDeviAction(Integer.parseInt(del_devi.get(i).get("id").toString()));
        	}
        }
        
        Map<String, Object> items = new HashMap<>();
        items.put("id", bh_id);
        
        result.data = items;
        
		return result;
	}
	
	
	@PostMapping("/mst_delete")
	public AjaxResult mstDelete(
			@RequestParam(value="bh_id", required=false, defaultValue= "0") Integer bhId,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();
		
		this.airConfresiaService.mstDelete(bhId);
		
		
		result.success = true;
		
		return result;
	}
	
	
	
	
	
	
	
	
	
	

}
