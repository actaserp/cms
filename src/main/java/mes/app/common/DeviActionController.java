package mes.app.common;

import java.sql.Date;
import java.sql.Timestamp;
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
import mes.domain.entity.ActionData;
import mes.domain.entity.DeviationAction;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.ActionDataRepository;
import mes.domain.repository.DeviationActionRepository;
import mes.domain.services.CommonUtil;
//import mes.domain.services.DateUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/common/devi_action")
public class DeviActionController {

	@Autowired
	private DeviActionService deviActionService;

	@Autowired
	DeviationActionRepository deviationActionRepository;
	
	@Autowired
	ActionDataRepository actionDataRepository;
	
	@Autowired
	SqlRunner sqlRunner;
	
	// 이상발생 내역 조회 
	@GetMapping("/check_devi_action_list")
	public AjaxResult getCheckDeviActionList(
    		@RequestParam(value="date_from", required=false) String date_from, 
    		@RequestParam(value="date_to", required=false) String date_to,
    		@RequestParam(value="table_name", required=false) String table_name,
    		@RequestParam(value="state", required=false) String state,
			HttpServletRequest request) {

        List<Map<String, Object>> items = this.deviActionService.getCheckDeviActionList(date_from, date_to, table_name, state);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	// ccp 이상발생 내역 조회
	@GetMapping("/ccp_devi_action_list")
	public AjaxResult getCcpDeviActionList(
    		@RequestParam(value="date_from", required=false) String date_from, 
    		@RequestParam(value="date_to", required=false) String date_to,
    		@RequestParam(value="haccp_code", required=false) String haccpCode,
    		@RequestParam(value="equip_id", required=false) String equipId,
			HttpServletRequest request) {

        List<Map<String, Object>> items = this.deviActionService.getCcpDeviActionList(date_from, date_to, haccpCode, equipId);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@GetMapping("/devi_action_list")
	public AjaxResult getDeviActionList(
    		@RequestParam(value="data_pk", required=false) Integer data_pk,
    		@RequestParam(value="table_name", required=false) String table_name,
			HttpServletRequest request) {

        List<Map<String, Object>> items = this.deviActionService.getDeviActionList(data_pk, table_name);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@GetMapping("/devi_action_detail")
	public AjaxResult getDeviActionDetailList(
    		@RequestParam(value="id", required=false) Integer id,
			HttpServletRequest request) {

        Map<String, Object> items = this.deviActionService.getDeviActionDetailList(id);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@PostMapping("/save_abnormal_detail")
	public AjaxResult saveAbnormalDetail(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam(value="table_name", required=false) String table_name,
			@RequestParam(value="data_pk", required=false) Integer data_pk,
			@RequestParam(value="happen_date", required=false) String happen_date,
			@RequestParam(value="happen_place", required=false) String happen_place,
			@RequestParam(value="abnormal_detail", required=false) String abnormal_detail,
			HttpServletRequest request,
			Authentication auth) {
        
        AjaxResult result = new AjaxResult();
        
		User user = (User)auth.getPrincipal();
		
		DeviationAction da = null;
		
		if (id == null) {
			da = new DeviationAction();		
		} else {
			da = this.deviationActionRepository.getDeviationActionById(id);
		}
		
        da.setSourceDataPk(data_pk);
        da.setSourceTableName(table_name);
        da.setHappenDate(Date.valueOf(happen_date));
        da.setHappenPlace(happen_place);
        da.setAbnormalDetail(abnormal_detail);
        da.set_audit(user);
	
        da = this.deviationActionRepository.save(da);
		
		return result;
	}
	@PostMapping("/save_dev_action")
	@Transactional
	public AjaxResult saveDevAction(
    		@RequestParam(value="id", required=false) Integer id, 
    		@RequestParam(value="source_pk", required=false) Integer sourcePk,
    		@RequestParam(value="source_table_name", required=false) String sourceTableName,
    		@RequestParam(value="happen_date", required=false) String happenDate,
    		@RequestParam(value="abnormal_detail", required=false) String abnormalDetail,
    		@RequestParam(value="action_detail", required=false) String actionDetail,
    		@RequestParam(value="confirm_detail", required=false) String confirmDetail,
			HttpServletRequest request,
			Authentication auth
			) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
		Timestamp today = new Timestamp(System.currentTimeMillis());
		
		DeviationAction da = new DeviationAction();
		
		if (id > 0) {
			da = this.deviationActionRepository.getDeviationActionById(id);
		}
		
		da.setSourceDataPk(sourcePk);
		da.setSourceTableName(sourceTableName);
		da.setHappenDate(Date.valueOf(happenDate));
		da.setAbnormalDetail(abnormalDetail);
		da.setActionDetail(actionDetail);
		da.setConfirmDetail(confirmDetail);
		da.setActionState("Y");
		da.setConfirmState("Y");
		da.set_audit(user);
		
		this.deviationActionRepository.save(da);
		
        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("id", id);
        
        String sql = """
        		delete from action_data where "DataPk" = :id and "TableName" = 'devi_action'
        		""";
		
        this.sqlRunner.execute(sql, dicParam);
        
        ActionData ac = new ActionData();
        ac.setDataPk(da.getId());
        ac.setTableName("devi_action");
        ac.setCode("action");
        ac.setActorPk(user.getId());
        ac.setActionDateTime(today);
        ac.setActorTableName("auth_user");
        ac.setActorName(user.getUserProfile().getName());
        ac.set_audit(user);
        
        this.actionDataRepository.save(ac);
        
        ActionData cf = new ActionData();
        cf.setDataPk(da.getId());
        cf.setTableName("devi_action");
        cf.setCode("confirm");
        cf.setActorPk(user.getId());
        cf.setActionDateTime(today);
        cf.setActorTableName("auth_user");
        cf.setActorName(user.getUserProfile().getName());
        cf.set_audit(user);
        
        this.actionDataRepository.save(cf);
        
        result.success = true;
        
        return result;
	}
	
	
	@SuppressWarnings("unused")
	@PostMapping("/save_multi_devi_action")
	public AjaxResult saveMultiDeviAction(
    		@RequestParam(value="data_pk", required=false) Integer dataPk, 
    		@RequestParam(value="table_name", required=false) String tableName,
    		@RequestParam(value="happen_date", required=false) String happenDate,
    		@RequestParam(value="happen_place", required=false) String happenPlace,
    		@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
	    List<Map<String, Object>> data = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
	    
	    Timestamp date = CommonUtil.tryTimestamp(happenDate);
	    
	    for (int i = 0; i < data.size(); i++) {
	    	Integer id = Integer.parseInt(data.get(i).get("id").toString());
	    	Integer srcDataPk = Integer.parseInt(data.get(i).get("src_data_pk").toString());
	    	String abnormalDetail = data.get(i).get("abnormal_detail") != null ? data.get(i).get("abnormal_detail").toString() : "";
	    	String actionDetail = data.get(i).get("action_detail") != null ? data.get(i).get("action_detail").toString() : "";
	    	String confirmDetail = data.get(i).get("confirm_detail") != null ? data.get(i).get("confirm_detail").toString() : "";
	    	String actionState = null;
	    	
	    	if(!actionDetail.isEmpty()) {
	    		actionState = "Y";
	    	} else {
	    		actionState = "N";
	    	}
	    	String confirmState = "N";
	    	
	    	DeviationAction da = null;
	    	if (id != null) {
	    		da = this.deviationActionRepository.getDeviationActionById(id);
	    		da.setHappenDate(date);
	    		da.setHappenPlace(happenPlace);
	    	} else {
	    		da = new DeviationAction();
	    		da.setSourceDataPk(srcDataPk);
	    		da.setSourceTableName(tableName);
	    		da.setHappenDate(date);
	    		da.setHappenPlace(happenPlace);
	    	}
	    	da.setAbnormalDetail(abnormalDetail);
	    	da.setConfirmDetail(confirmDetail);
	    	da.setActionDetail(actionDetail);
	    	da.setActionState(actionState);
	    	da.setConfirmState(confirmState);
	    	da.set_audit(user);
	    	
	    	da = this.deviationActionRepository.save(da);
	    }
	    return result;
	}
}
