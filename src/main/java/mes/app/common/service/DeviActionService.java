package mes.app.common.service;

import java.sql.Date;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import mes.domain.entity.ActionData;
import mes.domain.entity.DeviationAction;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.ActionDataRepository;
import mes.domain.repository.DeviationActionRepository;
import mes.domain.services.DateUtil;
import mes.domain.services.SqlRunner;

@Service
public class DeviActionService {

	@Autowired
	SqlRunner sqlRunner;
	
	@Autowired
	private DeviationActionRepository deviationActionRepository;
	
	@Autowired
	private ActionDataRepository actionDataRepository;
	
	// 이상발생 내역 조회
	public List<Map<String, Object>> getCheckDeviActionList(String date_from, String date_to, String table_name, String state) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("date_from", Date.valueOf(date_from));
		paramMap.addValue("date_to", Date.valueOf(date_to));
		paramMap.addValue("table_name", table_name);
		
        String sql = """
        		select da.id
                , cm."Name" as check_master_name
                , cr.id as check_result_id
	            , fn_appr_state('check_result', cr.id) as appr_state
                , da."HappenDate" as happen_date, da."HappenPlace" as happen_place
                , da."AbnormalDetail" as abnormal_detail, da."ActionDetail" as action_detail
                , da."ActionState" as action_state, da."ConfirmState" as confirm_state
	            from devi_action da
                left join check_result cr on cr.id = da."SourceDataPk"
	            left join check_mast cm on cm.id = cr."CheckMaster_id"
	            where 1 = 1
                and da."HappenDate" between :date_from and :date_to
        		""";
  		
	    if (StringUtils.hasText(table_name)) {
	    	sql +=" and da.\"SourceTableName\" = :table_name ";
	    }
	    
	    if ("need_action".equals(state)) {
	    	sql += " and da.\"ActionState\" is null ";
	    } else if ("need_confirm".equals(state)) {
	    	sql += """
	    			and da."ActionState" = 'Y' 
                    and da."ConfirmState" is null 
	    			""";
	    }
	    
        sql += " order by da.\"HappenDate\"  ";
       
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

        return items;
	}
	
	public List<Map<String, Object>> getCcpDeviActionList(String date_from, String date_to, String haccpCode,
			String equipId) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("dateFrom", Date.valueOf(date_from));
		paramMap.addValue("dateTo", Date.valueOf(date_to));
		paramMap.addValue("haccpCode", haccpCode);
		paramMap.addValue("equipId", equipId);
		
		String sql = """
					select  
					hp."Name" as "haccpName"
					,e."Name" as "equipName"
					,m."Code" as "matCode"
					,m."Name" as "matName"
					,hddd."HappenTime" as "happenTime"
					,hddd."AbnormalDetail" as "abnormalDetail"
					,hddd."ActionDetail" as "actionDetail" 
					,hddd."ActorName"  as "actorName"
					,hddd."Description" as "description" 
					,up."Name" as "writer"
					from haccp_diary_devi_detect hddd
					left join haccp_test ht ON ht.id = hddd."HaccpTest_id"
					left join haccp_diary hd on hddd."HaccpDiary_id"  = hd.id
					left join haccp_proc hp on hd."HaccpProcess_id" = hp.id
					left join haccp_item hi on hi.id =hddd."HaccpItem_id" 
					left join material m on m.id = ht."Material_id"
					left join auth_user au on au.id = hddd."_creater_id" 
					left join user_profile up on up."User_id" =au.id
					left join equ e on e.id=ht."Equipment_id"
					where 1=1
					and hd."DataDate" between :dateFrom and :dateTo
					""";
		
		if(StringUtils.hasText(haccpCode)) {
			sql += " and  hp.id = cast(:haccpCode as Integer) ";
		}
		
		if(StringUtils.hasText(equipId)) {
			sql += " and e.id = cast(:equipId as Integer) ";
		}
		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

        return items;
	}
	
	public List<Map<String, Object>> getDeviActionList (Integer data_pk, String table_name) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("data_pk", data_pk);
        dicParam.addValue("table_name", table_name);
        
        String sql = """
        		select da.id, da."HappenDate" as happen_date, da."HappenPlace" as happen_place
	            , da."AbnormalDetail" as abnormal_detail, da."ActionDetail" as action_detail
	            , da."ActionState" as action_state, da."ConfirmState" as confirm_state
		        from devi_action da
		        where 1=1
	            and da."SourceDataPk" = :data_pk
		        and da."SourceTableName" = :table_name
		        order by da.id
        		""";
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

        return items;
	}
	
	public Map<String, Object> getDeviActionDetailList(Integer id){
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("id", id);
        
        String sql = """
        		select da.id, da."HappenDate" as happen_date, da."HappenPlace" as happen_place
	            , da."AbnormalDetail" as abnormal_detail, da."ActionDetail" as action_detail
	            , da."ActionState" as action_state, da."ConfirmState" as confirm_state
		        from devi_action da
		        where 1=1
	            and da.id = :id
		    """;
        
        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
        
        return item;
	}
	
	//선행일지에서 발생내역, 조치내역, 확인내역 일괄등록(즉시조치)
	public AjaxResult saveDeviAction(Integer id, Integer source_pk,
			String source_table_name, String happen_date, String happen_place,
			String abnormal_detail, String action_detail, String confirm_detail, User user){
		        
        DeviationAction da = new DeviationAction();
        
        if(id > 0) {
        	da = this.deviationActionRepository.getDeviationActionById(id);
        }
        da.setSourceDataPk(source_pk);
        da.setSourceTableName(source_table_name);
        da.setHappenDate(Date.valueOf(happen_date));
        da.setHappenPlace(happen_place);
        da.setAbnormalDetail(abnormal_detail);
        da.setActionDetail(action_detail);
        da.setConfirmDetail(confirm_detail);
        da.setActionState("Y");
        da.setConfirmState("Y");
        da.set_audit(user);
        da = this.deviationActionRepository.save(da);
        
        //인원등록
        MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("id", id);
        
        String sql = """
    		delete from action_data where "DataPk" = :id and "TableName" = 'devi_action'
		    """;
        
        this.sqlRunner.execute(sql, dicParam);
        
        ActionData actor = new ActionData();
        actor.setDataPk(da.getId());
        actor.setTableName("devi_action");
        actor.setCode("action");
        actor.setActorPk(user.getId());
        actor.setActionDateTime(DateUtil.getNowTimeStamp());
        actor.set_audit(user);
        actor = this.actionDataRepository.save(actor);
        
        ActionData confer = new ActionData();
        confer.setDataPk(da.getId());
        confer.setTableName("devi_action");
        confer.setCode("confirm");
        confer.setActorPk(user.getId());
        confer.setActionDateTime(DateUtil.getNowTimeStamp());
        confer.set_audit(user);
        confer = this.actionDataRepository.save(confer);
        
        AjaxResult result = new AjaxResult();
        result.data = confer;
		return result;
	}

	//선행일지에서 일괄삭제
	public AjaxResult deleteDeviAction(Integer id){
		        
        this.deviationActionRepository.deleteById(id);
        
        //인원등록
        MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("id", id);
        
        String sql = """
    		delete from action_data where "DataPk" = :id and "TableName" = 'devi_action'
		    """;
        
        this.sqlRunner.execute(sql, dicParam);
        
        AjaxResult result = new AjaxResult();
        result.success = true;
		return result;
	}

}
