package mes.app.precedence;

import java.sql.Date;
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
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.common.service.DeviActionService;
import mes.app.precedence.service.PlaceOutCheckListService;
import mes.domain.entity.BundleHead;
import mes.domain.entity.CheckItemResult;
import mes.domain.entity.CheckMaster;
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
@RequestMapping("/api/precedence/place_out_check_list")
public class PlaceOutCheckListController {
	
	@Autowired
	private PlaceOutCheckListService placeOutCheckListService;
	
	@Autowired
	private DeviActionService deviActionService;
	
	@Autowired
	CheckMasterRepository checkMasterRepository; 
	
	@Autowired
	CheckResultRepository checkResultRepository;
	
	@Autowired
	CheckItemResultRepository checkItemResultRepository;

	@Autowired
	BundleHeadRepository bundleHeadRepository;
	
	@Autowired
	SqlRunner sqlRunner;

	// 결재현황 조회
	@GetMapping("/appr_stat")
	public AjaxResult getPlaceOutCheckListApprStat(
    		@RequestParam(value="start_date", required=false) String start_date, 
    		@RequestParam(value="end_date", required=false) String end_date,
    		@RequestParam(value="appr_state", required=false) String appr_state,
			HttpServletRequest request) {
		
        List<Map<String, Object>> items = this.placeOutCheckListService.getPlaceOutCheckListApprStat(start_date, end_date, appr_state);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	// 점검표 조회
	@GetMapping("/read")
	public AjaxResult getPlaceOutCheckList(
    		@RequestParam(value="bh_id", required=false) Integer bh_id, 
			HttpServletRequest request) {
		
        Map<String, Object> items = this.placeOutCheckListService.getPlaceOutCheckList(bh_id);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	// 점검표 삭제
	@Transactional
	@PostMapping("/delete")
	public AjaxResult deletePlaceOutCheckList(
    		@RequestParam(value="bh_id", required=false) Integer bh_id, 
    		HttpServletRequest request,
			Authentication auth) {
		      
        AjaxResult result = new AjaxResult();
                
        this.bundleHeadRepository.deleteById(bh_id);
        
        List<CheckResult> crList = this.checkResultRepository.findBySourceDataPk(bh_id);
        if (!crList.isEmpty()) {
            for (CheckResult cr : crList) {
                this.checkItemResultRepository.deleteByCheckResultId(cr.getId());
            }
        }
        this.checkResultRepository.deleteBySourceDataPk(bh_id);
        
        result.success = true;        
		return result;
	}
		
	@PostMapping("/save")
	public AjaxResult savePlaceOutCheckList (
			@RequestParam(value="bh_id", required=false) Integer bh_id,
			@RequestParam(value="title", required=false) String title,
			@RequestParam(value="data_date", required=false) String data_date,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
		CheckMaster cm = this.checkMasterRepository.getByCode(title);
		Integer check_master_id = cm.getId();
		
		BundleHead bh = new BundleHead();
		
		if (bh_id > 0) {
			bh = this.bundleHeadRepository.getBundleHeadById(bh_id);
			bh.setTableName("place_out_check_list");
			bh.setDate1(CommonUtil.tryTimestamp(data_date));
			bh.setChar1(title);
			bh.set_audit(user);
			bh = this.bundleHeadRepository.save(bh);
			
			CheckResult cr = this.checkResultRepository.getCheckResultBySourceDataPk(bh_id);
			Integer check_result_id = cr.getId();
			
		    List<Map<String, Object>> qItems = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		    
		    if (qItems.size() == 0) {
				result.success = false;
				return result;
			}
		    
		    for (int i = 0; i < qItems.size(); i++) {
		    	Integer pk = CommonUtil.tryIntNull(qItems.get(i).get("id"));
		    	String result1 = CommonUtil.tryString(qItems.get(i).get("result1"));
		    	
		    	CheckItemResult cir = this.checkItemResultRepository.findByCheckResultIdAndCheckItemId(check_result_id, pk);
		    	cir.setResult1(result1);
		        cir.set_audit(user);
		        cir = this.checkItemResultRepository.save(cir);
		    }
		    
		    MapSqlParameterSource dicParam = new MapSqlParameterSource();        
	        dicParam.addValue("check_result_id", check_result_id);   
	        dicParam.addValue("bh_id", bh.getId());
	        
	        String sql = """
	        	select cir.id as src_data_pk ,'place_out_check_list' as source_table_name , cr."CheckDate" as happen_date, cm."Name" as happen_place
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
                inner join check_result c on b."CheckResult_id" = c.id and c."SourceDataPk" = :bh_id
	            where a."SourceTableName" = 'place_out_check_list' )
                and cir."Result1" = 'X'
        		""";
	  			    
	        List<Map<String, Object>> devi = this.sqlRunner.getRows(sql, dicParam);
	        if (devi.size() > 0) {
	            for (Map<String, Object> item : devi) {
	                Integer srcDataPk = CommonUtil.tryIntNull(item.get("src_data_pk"));
	                String sourceTableName = CommonUtil.tryString(item.get("source_table_name"));
	                String happenDate = CommonUtil.tryString(item.get("happen_date"));
	                String happenPlace = CommonUtil.tryString(item.get("happen_place"));
	                String abnormalDetail = CommonUtil.tryString(item.get("abnormal_detail"));
	                String actionDetail = CommonUtil.tryString(item.get("action_detail"));
	                String confirmDetail = CommonUtil.tryString(item.get("confirm_detail"));
	                this.deviActionService.saveDeviAction(0, srcDataPk, sourceTableName, happenDate, happenPlace,  abnormalDetail, actionDetail, confirmDetail, user);
	            }
	        }
	        
	        sql = """
	        	select id from devi_action da
                    where da."SourceTableName" = 'place_out_check_list'
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
	        
	        List<Map<String, Object>> del_devi = this.sqlRunner.getRows(sql, dicParam);
	        if (del_devi.size() > 0) {
	            for (Map<String, Object> item : del_devi) {
	                Integer id = CommonUtil.tryIntNull(item.get("id"));
	                this.deviActionService.deleteDeviAction(id);
	            }
	        }
	        
	        Map<String,Object> item = new HashMap<String,Object>();
			item.put("id", bh.getId());
		    result.success=true;
		    result.data=item;
		}
		else {
			bh.setTableName("place_out_check_list");
			bh.setDate1(CommonUtil.tryTimestamp(data_date));
			bh.setChar1(title);
			bh.set_audit(user);
			bh = this.bundleHeadRepository.save(bh);
			
			CheckResult cr = new CheckResult();
			cr.setCheckMasterId(check_master_id);
			cr.setCheckDate(Date.valueOf(data_date));    
			cr.setCheckerName(user.getUserProfile().getName());
			cr.setSourceDataPk(bh.getId());
			cr.setSourceTableName("bundle_bead");
			cr.set_audit(user);
			cr = this.checkResultRepository.save(cr);
			Integer check_result_id = cr.getId();
			
			List<Map<String, Object>> qItems = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		    
		    if (qItems.size() <= 0) {
				result.success = false;
				return result;
			}
		    
		    for (int i = 0; i < qItems.size(); i++) {
		    	CheckItemResult cir = new CheckItemResult();
		    	Integer checkitem_id = CommonUtil.tryIntNull(qItems.get(i).get("id"));
		    	String result1 = CommonUtil.tryString(qItems.get(i).get("result1"));
		    	Integer order = CommonUtil.tryIntNull(qItems.get(i).get("order"));
		    	
		    	cir.setCheckResultId(check_result_id);
		    	cir.setCheckItemId(checkitem_id);
		    	cir.setResult1(result1);
		    	cir.setOrder(order);
		        cir.set_audit(user);
		        cir = this.checkItemResultRepository.save(cir);
		    }
		    
		    MapSqlParameterSource dicParam = new MapSqlParameterSource();        
	        dicParam.addValue("check_result_id", check_result_id);
	        
		    String sql = """
	        	select cir.id as src_data_pk ,'place_out_check_list' as source_table_name , cr."CheckDate" as happen_date, cm."Name" as happen_place
	            , concat('부적합 : ', coalesce(ci."ItemGroup1",' '), coalesce(ci."ItemGroup2", ' '), coalesce(ci."ItemGroup3",' '), ci."Name") as abnormal_detail
	            ,null as action_detail ,null as confirm_detail
	            from check_result cr
	            inner join check_item_result cir on cir."CheckResult_id" = cr.id
	            inner join check_item ci on ci.id = cir."CheckItem_id"
	            inner join check_mast cm on cm.id = cr."CheckMaster_id"	
	            where cr.id = :check_result_id
                and cir."Result1" = 'X'
        		""";
		    
		    List<Map<String, Object>> devi = this.sqlRunner.getRows(sql, dicParam);
		    if (devi.size() > 0) {
	            for (Map<String, Object> item : devi) {
	                Integer srcDataPk = CommonUtil.tryIntNull(item.get("src_data_pk"));
	                String sourceTableName = CommonUtil.tryString(item.get("source_table_name"));
	                String happenDate = CommonUtil.tryString(item.get("happen_date"));
	                String happenPlace = CommonUtil.tryString(item.get("happen_place"));
	                String abnormalDetail = CommonUtil.tryString(item.get("abnormal_detail"));
	                String actionDetail = CommonUtil.tryString(item.get("action_detail"));
	                String confirmDetail = CommonUtil.tryString(item.get("confirm_detail"));
	                this.deviActionService.saveDeviAction(0, srcDataPk, sourceTableName, happenDate,happenPlace,abnormalDetail, actionDetail, confirmDetail, user);
	            }
	        }
		    
		    Map<String,Object> item = new HashMap<String,Object>();
			item.put("id", bh.getId());
		    result.success=true;
		    result.data=item;
		}
	    return result;

	}
	
}
