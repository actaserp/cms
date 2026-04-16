package mes.app.common.service;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import mes.domain.entity.ApprResult;
import mes.domain.entity.TaskMaster;
import mes.domain.entity.User;
import mes.domain.repository.ApprResultRepository;
import mes.domain.repository.TaskMasterRepository;
import mes.domain.services.DateUtil;
import mes.domain.services.SqlRunner;

@Service
public class ApprResultService {

	@Autowired
	SqlRunner sqlRunner;
	
	@Autowired
	ApprResultRepository apprResultRepository;
	
	@Autowired
	TaskMasterRepository taskMasterRepository;
	
	public Map<String, Object> getApprBoxList(Integer pk, String table_name, Integer user_id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("src_data_pk", pk);
		paramMap.addValue("src_table_name", table_name);
		
        String sql = """
        		select ar.id, ar."Line" as line, ar."LineName" as line_name
	            , ar."Approver_id" as approver_id, u."Name" as approver_name
	            , to_char(ar."ApprDate",'yyyy-mm-dd') as appr_date
                , ar."State" as state
                , ar."Description" as description
	            from appr_result ar
	            left join user_profile u on u."User_id" = ar."Approver_id"
	            where ar."SourceDataPk" = :src_data_pk
	            and ar."SourceTableName" = :src_table_name
	            order by  ar."Line" 
        		""";
  		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

        Map<String, Object> data = new HashMap<String, Object>(); 
        
        if (items != null) {
        	
    	    List<String> ar_id = new ArrayList<>();
    	    List<String> line_name = new ArrayList<>();
    	    List<Integer> approver_id = new ArrayList<>();
    	    List<String> approver_name = new ArrayList<>();
    	    List<String> appr_state = new ArrayList<>();
    	    List<String> appr_state_name = new ArrayList<>();
    	    List<String> appr_date = new ArrayList<>();
    	    List<String> description = new ArrayList<>();
        	for (int index = 0; index < items.size(); index++) {
        		
        		ar_id.add(items.get(index).get("id").toString());
        		line_name.add((String) items.get(index).get("line_name"));
                //approver_id.add(items.get(index).get("approver_id").toString());
        		approver_id.add((Integer) items.get(index).get("approver_id"));
                approver_name.add((String) items.get(index).get("approver_name"));
                String state = (String) items.get(index).get("state");
                String state_name = "";
                if (index == 0) {
                	state_name = "작성";
                } else {
                	if ("Y".equals(state)) {
                		state_name = "승인";
                	} else if ("N".equals(state)) {
                		state_name = "기각";
                	} else {
                		state_name = "";
                	}
                }
                appr_state.add(state);
                appr_state_name.add(state_name);
                appr_date.add((String) items.get(index).get("appr_date"));
                description.add((String) items.get(index).get("description"));                
        	}
        	data.put("line_count", items.size());
        	data.put("user_id", user_id);
            data.put("ar_id", ar_id);
            data.put("line_name", line_name);
            data.put("approver_id", approver_id);
            data.put("approver_name", approver_name);
            data.put("appr_state", appr_state);
            data.put("appr_state_name", appr_state_name);
            data.put("appr_date", appr_date);
            data.put("description", description);
        }        
        
        return data;
	}

	public List<Map<String, Object>> getToApproveList(Integer user_id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("user_id", user_id);
		
        String sql = """
        		select ar.id, ar."SourceDataPk" as data_pk
	            , ar."SourceTableName" as table_name
	            , cm."Name" as data_name 
	            , ar."LineName" as line_name
	            , u."Name" as approver_name
	            , ar._created 
		        from appr_result ar
	            left join user_profile u on u."User_id" = ar."Approver_id"
	            left join check_result cr on cr.id = ar."SourceDataPk"
	            and ar."SourceTableName" = 'check_result'
	            left join check_mast cm on cm.id = cr."CheckMaster_id"
		        where "Approver_id" = :user_id
		        and ar."State" is null
	            and ar."Line" in (select max(ar2."Line") + 1
					from appr_result ar2
					where ar2."SourceTableName" = ar."SourceTableName"
					and ar2."SourceDataPk" = ar."SourceDataPk"
					and ar2."State" = 'Y'
					)
		        order by ar._created
        		""";
  		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

        return items;
	}

	@Transactional
	public boolean ApproverHeadInsert(String taskCode, Integer pk, String table, Integer userId, String linkTitle,
			String linkGui, String linkGuiParam, User user) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("srcPk", pk);
		paramMap.addValue("srcTableName", table);
		
		String sql = """
				update appr_result set _status = 'T' 
				where "SourceDataPk" = :srcPk and "SourceTableName" = :srcTableName 
				""";
		
		this.sqlRunner.execute(sql, paramMap);
		
		Timestamp today = new Timestamp(System.currentTimeMillis());
		
		ApprResult ar = new ApprResult();
		ar.setSourceDataPk(pk);
		ar.setSourceTableName(table);
		ar.setLine(-1);
		ar.setLineName("헤더");
		ar.setDescription(taskCode);
		ar.setApproverId(userId);
		ar.setApprDate(today);
		ar.setState("process");
		ar.setOriginTableName(linkTitle);
		ar.setOriginGui(linkGui);
		ar.setOriginGuiParam(linkGuiParam);
		ar.set_status("A");
		ar.set_audit(user);
		
		this.apprResultRepository.save(ar);
		
		boolean result = true;
		
		return result;
	}

	@Transactional
	public boolean ApprItemInsert(String taskCode, Integer srcPk, String srcTableName, Integer userId, String[] approverList, User user) {
		
		TaskMaster tm = this.taskMasterRepository.findTaskMasterByCode(taskCode);
		Timestamp today = new Timestamp(System.currentTimeMillis());
		
		ApprResult ar = new ApprResult();
		ar.setSourceDataPk(srcPk);
		ar.setSourceTableName(srcTableName);
		ar.setLine(0);
		ar.setLineName("작성");
		ar.setApproverId(userId);
		ar.setApprDate(today);
		ar.setApprStepYN("N");
		ar.setState("approval");
		ar.set_status("A");
		ar.set_audit(user);
		
		this.apprResultRepository.save(ar);
		
		if (approverList.length > 0) {
			if(!tm.getLine1Name().isEmpty() && !approverList[0].isEmpty()) {
				ApprResult ar1 = new ApprResult();
				ar1.setSourceDataPk(srcPk);
				ar1.setSourceTableName(srcTableName);
				ar1.setLine(1);
				ar1.setLineName(tm.getLine1Name());
				ar1.setApproverId(Integer.parseInt(approverList[0]));
				ar1.setApprStepYN("Y");
				ar1.set_status("A");
				ar1.set_audit(user);
				
				this.apprResultRepository.save(ar1);
			}
		}
		
		if (approverList.length > 1) {
			if(!tm.getLine2Name().isEmpty() && !approverList[1].isEmpty()) {
				ApprResult ar2 = new ApprResult();
				ar2.setSourceDataPk(srcPk);
				ar2.setSourceTableName(srcTableName);
				ar2.setLine(2);
				ar2.setLineName(tm.getLine2Name());
				ar2.setApproverId(Integer.parseInt(approverList[1]));
				ar2.setApprStepYN("N");
				ar2.set_status("A");
				ar2.set_audit(user);
				
				this.apprResultRepository.save(ar2);
			}
		}
		
		if (approverList.length > 2) {
			if(!tm.getLine3Name().isEmpty() && !approverList[2].isEmpty()) {
				ApprResult ar3 = new ApprResult();
				ar3.setSourceDataPk(srcPk);
				ar3.setSourceTableName(srcTableName);
				ar3.setLine(3);
				ar3.setLineName(tm.getLine3Name());
				ar3.setApproverId(Integer.parseInt(approverList[2]));
				ar3.setApprStepYN("N");
				ar3.set_status("A");
				ar3.set_audit(user);
				
				this.apprResultRepository.save(ar3);
			}
		}
		
		if (approverList.length > 3) {
			if(!tm.getLine4Name().isEmpty() && !approverList[3].isEmpty()) {
				ApprResult ar4 = new ApprResult();
				ar4.setSourceDataPk(srcPk);
				ar4.setSourceTableName(srcTableName);
				ar4.setLine(4);
				ar4.setLineName(tm.getLine4Name());
				ar4.setApproverId(Integer.parseInt(approverList[3]));
				ar4.setApprStepYN("N");
				ar4.set_status("A");
				ar4.set_audit(user);
				
				this.apprResultRepository.save(ar4);
			}
		}
		
		boolean result = true;
		
		return result;
	}

	
	public boolean confirmApprove(Integer src_pk, String src_table_name, User user, String state, String desc) {
		
		//결재정보수정
		Integer user_id = user.getId();
		ApprResult ar = this.apprResultRepository.getBySourceDataPkAndSourceTableNameAndApproverIdAndApprStepYN(src_pk, src_table_name, user_id, "Y");
		ar.setApprDate(DateUtil.getNowTimeStamp());
		ar.setState(state);
		ar.setDescription(desc);
		ar.setApprStepYN("N");
		ar.set_audit(user);
		this.apprResultRepository.save(ar);
		
		
		//승연여부에 따른 다음 결재자 변경
		if("approval".equals(state)) {
			MapSqlParameterSource paramMap = new MapSqlParameterSource();
			paramMap.addValue("src_pk", src_pk);
			paramMap.addValue("src_table_name", src_table_name);
			
			String sql = """
					with t as (
		                select id, rank() over(order by "Line") as rn
		                from appr_result
		                where "ApprDate" is null
		                and "SourceDataPk" = :src_pk
		                and "SourceTableName" = :src_table_name
	                    and _status = 'A'
	                )
	                update appr_result
	                set "ApprStepYN" = 'Y'
	                from t
	                where appr_result.id = t.id
	                and t.rn = 1
	                and appr_result._status = 'A'
					""";
			
			this.sqlRunner.execute(sql, paramMap);
		}else {
			MapSqlParameterSource paramMap = new MapSqlParameterSource();
			paramMap.addValue("src_pk", src_pk);
			paramMap.addValue("src_table_name", src_table_name);
			
			String sql = """
					update appr_result
	                set "ApprDate" = now()
		                , "State" = 'reject'
		                , "Description" = '이전 결재 반려에 따른 자동 반려'
	                where "SourceDataPk" = :src_pk
	                and "SourceTableName" = :src_table_name
	                and "ApprDate" is null
	                and _status = 'A'
					""";
			
			this.sqlRunner.execute(sql, paramMap);
		}
		
		//최종 결재상태 변경
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("src_pk", src_pk);
		paramMap.addValue("src_table_name", src_table_name);
		
		String sql = """
				with t as (
		            select "SourceDataPk", "SourceTableName", max("Line") over() as max_line, "Line"
			            , coalesce("State", 'process') as "State"
		            from appr_result
		            where "SourceDataPk" = :src_pk
		            and "SourceTableName" = :src_table_name
	                and _status = 'A'
	            )
	            update appr_result
	            set "State" = t."State"
	            from t
	            where appr_result."SourceDataPk" = t."SourceDataPk"
	            and appr_result."SourceTableName" = t."SourceTableName"
	            and appr_result."Line" = -1
	            and appr_result._status = 'A'
	            and t.max_line = t."Line"
				""";
		
		this.sqlRunner.execute(sql, paramMap);
				
		boolean result = true;
		
		return result;
	}
	
	
	

	public Integer getApproverCheck(Integer pk, String table, Integer headId) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("pk", pk);
		paramMap.addValue("table", table);
		paramMap.addValue("headId", headId);
		
		String sql = """
	            select case when count(*)=0 then 0 else max(r.id) end as "check"
	            from appr_result r
	            where r."Line" = -1
				and r._status = 'A'
	            and (
		            (
			            r."SourceDataPk" = :pk
			            and r."SourceTableName" = :table
			        )
		            or exists (
			            select 1
			            from appr_result 
			            where id = :headId
			            and "Line" = -1
			            and "SourceDataPk" = r."SourceDataPk"
			            and "SourceTableName" = r."SourceTableName"
		            )
	            )
				""";
		
		Integer cnt = this.sqlRunner.queryForCount(sql, paramMap);
		
		return cnt;
	}

	public Map<String,Object> getApproverDefLine(String taskCode, Integer deptId, String shift) {
		TaskMaster tm = taskMasterRepository.findTaskMasterByCode(taskCode);
		Map<String,Object> item = new HashMap<String, Object>();
		if (tm != null) {
			Map<String,Object> taskMaster = new HashMap<String, Object>();
			taskMaster.put("id", tm.getId());
			taskMaster.put("Code", tm.getCode());
			taskMaster.put("TaskName", tm.getTaskName());
			taskMaster.put("Line1Name", tm.getLine1Name());
			taskMaster.put("Line2Name", tm.getLine2Name());
			taskMaster.put("Line3Name", tm.getLine3Name());
			taskMaster.put("Line4Name", tm.getLine4Name());
			
			item.put("TaskMaster", taskMaster);
			
			Map<String,Object> ApprHead = new HashMap<String, Object>();
			ApprHead.put("id", 0);
			ApprHead.put("State", "write");
			ApprHead.put("SourceDataPk", 0);
			ApprHead.put("SourceTableName", "");
			ApprHead.put("OriginTableName", "");
			ApprHead.put("OriginGui", "");
			ApprHead.put("OriginGuiParam", "");
			
			item.put("ApprHead", ApprHead);
			
			
			List<Map<String,Object>> line0 = this.getApproverDefLineByStep(taskCode, deptId, shift, 0);
			
			item.put("Line0", line0);
			
			List<Map<String,Object>> line1 = this.getApproverDefLineByStep(taskCode, deptId, shift, 1);
			
			item.put("Line1", line1);
			
			List<Map<String,Object>> line2 = this.getApproverDefLineByStep(taskCode, deptId, shift, 2);
			
			item.put("Line2", line2);
			
			List<Map<String,Object>> line3 = this.getApproverDefLineByStep(taskCode, deptId, shift, 3);
			
			item.put("Line3", line3);
			
			List<Map<String,Object>> line4 = this.getApproverDefLineByStep(taskCode, deptId, shift, 4);
			
			item.put("Line4", line4);
		
		} else {
			return null;
		}
		
		return item;
		
	}

	public Map<String,Object> getApproverLine(String taskCode, Integer deptId, String shift, Integer pk, String table, Integer headId,	Integer userId) {
		
		// 업무별 기본 결재선 정보 조회 
		headId = this.getApproverCheck(pk,table, headId);
		
		TaskMaster tm = taskMasterRepository.findTaskMasterByCode(taskCode);
		
		ApprResult ar = apprResultRepository.findApprResultById(headId);
		
		
		Map<String,Object> item = new HashMap<String, Object>();
		if (tm != null && ar != null) {
			Map<String,Object> taskMaster = new HashMap<String, Object>();
			taskMaster.put("id", tm.getId());
			taskMaster.put("Code", tm.getCode());
			taskMaster.put("TaskName", tm.getTaskName());
			taskMaster.put("Line1Name", tm.getLine1Name());
			taskMaster.put("Line2Name", tm.getLine2Name());
			taskMaster.put("Line3Name", tm.getLine3Name());
			taskMaster.put("Line4Name", tm.getLine4Name());
			
			item.put("TaskMaster", taskMaster);
			
			Map<String,Object> ApprHead = new HashMap<String, Object>();
			ApprHead.put("id", ar.getId());
			ApprHead.put("State", ar.getState());
			ApprHead.put("SourceDataPk", ar.getSourceDataPk());
			ApprHead.put("SourceTableName", ar.getSourceTableName());
			ApprHead.put("OriginTableName", ar.getOriginTableName());
			ApprHead.put("OriginGui", ar.getOriginGui());
			ApprHead.put("OriginGuiParam", ar.getOriginGuiParam());
			
			item.put("ApprHead", ApprHead);
			
			
			// 기안자
			List<Map<String,Object>> line0 = this.getApproverLineByStep(pk, table, headId, 0, userId);
			
			item.put("Line0", line0);
			
			List<Map<String,Object>> line1 = this.getApproverLineByStep(pk, table, headId, 1, userId);
			
			item.put("Line1", line1);
			
			List<Map<String,Object>> line2 = this.getApproverLineByStep(pk, table, headId, 2, userId);
			
			item.put("Line2", line2);
			
			List<Map<String,Object>> line3 = this.getApproverLineByStep(pk, table, headId, 3, userId);
			
			item.put("Line3", line3);
			
			List<Map<String,Object>> line4 = this.getApproverLineByStep(pk, table, headId, 4, userId);
			
			item.put("Line4", line4);
			
			
		   // 반려된 경우 결재선 변경할 수 있으므로 기본 결재선 추가
			Map<String,Object> defLine = new HashMap<String, Object>();
			
			List<Map<String,Object>> defLine1 = this.getApproverLineByStep(pk, table, headId, 1, userId);
			
			defLine.put("Line1", defLine1);
			
			List<Map<String,Object>> defLine2 = this.getApproverLineByStep(pk, table, headId, 2, userId);
			
			defLine.put("Line2", defLine2);
			
			List<Map<String,Object>> defLine3 = this.getApproverLineByStep(pk, table, headId, 3, userId);
			
			defLine.put("Line3", defLine3);
			
			List<Map<String,Object>> defLine4 = this.getApproverLineByStep(pk, table, headId, 4, userId);
			
			defLine.put("Line4", defLine4);
			
			item.put("DefLine", defLine);
		
		} else {
			return null;
		}
		
		return item;
		
	}
		public List<Map<String,Object>> getApproverLineByStep(Integer pk, String table, Integer headId, Integer lineNo, Integer userId) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("pk", pk);
		paramMap.addValue("table", table);
		paramMap.addValue("headId", headId);
		paramMap.addValue("lineNo", lineNo);
		paramMap.addValue("userId", userId);
		
		String sql = """
	            select r.id, r."Line", r."LineName", u."Depart_id", d."Name" as "DepartName", '' as "Shift", '' as "ShiftName", u."User_id", u."Name" as "UserName"
		            , coalesce(r."State", 'process') as "State", case when r."State" in ('approval', 'reject') then sc."Value" when r."ApprStepYN"='Y' then '진행중' else '-' end "StateName"
					, to_char(r."ApprDate", 'yyyy-MM-dd HH:mm:ss') as "ApprDate"
	                , coalesce(r."ApprStepYN", 'N') as "ApprStepYN", coalesce(r."Description", '') as "Description"
	                , case when r."ApprStepYN"='Y' and r."Approver_id"= :userId then 'Y' else 'N' end as "ApprUser"
	            from appr_result r
	            inner join user_profile u on r."Approver_id" = u."User_id"
	            left join depart d on u."Depart_id" = d.id
	            left join sys_code sc on r."State" = sc."Code" and sc."CodeType" = 'appr_status'
	            where r._status = 'A'
	            and r."Line" = :lineNo
	            and (
		            (
			            r."SourceDataPk" = :pk
			            and r."SourceTableName" = :table
		            )
		            or exists (
			            select 1
			            from appr_result 
			            where id = :headId
			            and "Line" = -1
			            and "SourceDataPk" = r."SourceDataPk"
			            and "SourceTableName" = r."SourceTableName"
		            )
	            )
				""";
		
		List<Map<String, Object>> item = this.sqlRunner.getRows(sql, paramMap);
		
		return item;
	}

	public List<Map<String,Object>> getApproverDefLineByStep(String taskCode, Integer deptId, String shift, Integer lineNo) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("taskCode", taskCode);
		paramMap.addValue("deptId", deptId);		
		paramMap.addValue("lineNo", lineNo);
		
		String sql = """
            select 0 as id, ta."Line"
	            , case ta."Line" when 1 then tm."Line1Name" when 2 then tm."Line2Name" when 3 then tm."Line3Name" else tm."Line4Name" end "LineName"
	            , ta."Depart_id", d."Name" as "DepartName", ta."Shift", case when ta."Shift"='D' then '주간' when ta."Shift"='N' then '야간' else null end as "ShiftName", ta."User_id", u."Name" as "UserName"
                , '' as "State", '-' as "StateName", '-' as "ApprDate"
                , 'N' as "ApprStepYN", '' as "Description"
                , 'N' as "ApprUser"
	            from task_master tm
	            inner join task_approver ta on tm.id = ta."TaskMaster_id"
	            left join depart d on ta."Depart_id" = d.id
	            left join user_profile u on ta."User_id" = u."User_id"
				where tm."Code" = :taskCode
                and ta."Line" = :lineNo 
				""";
		
		
		if (StringUtils.hasText(shift)) {
			paramMap.addValue("shift", shift);
			sql += " and (coalesce(ta.\"Shift\", '')='' or ta.\"Shift\" = :shift) ";
		}
		
		if (deptId != null) {
			sql += " and (coalesce(ta.\"Depart_id\", 0)=0 or ta.\"Depart_id\" = :deptId) ";
		}
		
		sql += " order by ta.\"Line\" ";
		
		List<Map<String, Object>> item = this.sqlRunner.getRows(sql, paramMap);
		
		return item;
	}

	public List<Map<String, Object>> myApproveList(String from_date, String to_date, String tm_id, String state, User user) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("from_date", from_date);
		paramMap.addValue("to_date", to_date);		
		paramMap.addValue("tm_id", tm_id);
		paramMap.addValue("state", state);
		
		Integer user_id = user.getId();
		
		paramMap.addValue("user_id", user_id);
		
		String sql = "";
		
		List<Map<String,Object>> items = null;
		
		if(state.equals("do_list")) {
			sql = """
					select r.id,hd.id ,b.id as bh_id,b."Text1", coalesce(b."Char1",tm."TaskName" )as "Title", r."State", to_char(r._created, 'yyyy-MM-dd HH:mm:ss') as "ReqDate", coalesce(r."StateName", '작성') as "StateName", r."LineName", r."LineNameState"
				, r."OriginGui", r."OriginGuiParam", r."OriginTableName", :state as "SearchState", 
				'N' as "CollectYN", tm."TaskName" as task_name
			from v_appr_result r
			left join bundle_head b on r."SourceDataPk" = b.id and r."SourceTableName" in('bundle_head','haccp_diary') 
			left join task_master tm on r."TaskMasterCode" = tm."Code"
			left join haccp_diary hd on hd.id = r."SourceDataPk"
			where 1=1
			and r._created between cast(:from_date as date) and cast(:to_date as date)+1
			and r."State" = 'process' 
			and ((:tm_id != '') and (tm.id = :tm_id::Integer) or (:tm_id = '') and (1=1))
			and exists (
				select 1
				from appr_result 
				where r."SourceDataPk" = "SourceDataPk"
				and r."SourceTableName" = "SourceTableName"
				and "Approver_id" = :user_id
				and "ApprStepYN" = 'Y'
				and "Line" > 0
			)
			order by r._created desc
					""";
			
			items = this.sqlRunner.getRows(sql, paramMap);
		}else if(state.equals("appr_list")){
			sql = """
					select r.id, b.id as bh_id, b."Char1" as "Title", r."State", to_char(r._created, 'yyyy-MM-dd HH:mm:ss') as "ReqDate", coalesce(r."StateName", '작성') as "StateName", r."LineName", r."LineNameState"
				, r."OriginGui", r."OriginGuiParam", r."OriginTableName", :state as "SearchState", 
				'N' as "CollectYN", tm."TaskName" as task_name
			from v_appr_result r
			inner join bundle_head b on r."SourceDataPk" = b.id and r."SourceTableName" in('bundle_head','haccp_diary') 
			left join task_master tm on r."TaskMasterCode" = tm."Code"
			where 1=1
			and r._created between cast(:from_date as date) and cast(:to_date as date)
			and r."State" in ('reprocess', 'process', 'approval', 'reject') 
			and ((:tm_id != '') and (tm.id = :tm_id::Integer) or (:tm_id = '') and (1=1))
			and exists (
				select 1
				from appr_result 
				where r."SourceDataPk" = "SourceDataPk"
				and r."SourceTableName" = "SourceTableName"
				and "Approver_id" = :user_id
				and "State" in ('approval', 'reject')
				and "Line" > 0
			)
			order by r._created desc
					""";
			
			items = this.sqlRunner.getRows(sql, paramMap);
		}else if(state.equals("req_list")) {
			sql = """
					select r.id, b.id as bh_id, b."Char1" as "Title", r."State", to_char(r._created, 'yyyy-MM-dd HH:mm:ss') as "ReqDate", coalesce(r."StateName", '작성') as "StateName", r."LineName", r."LineNameState"
				, r."OriginGui", r."OriginGuiParam", r."OriginTableName", :state as "SearchState", 
				'N' as "CollectYN", tm."TaskName" as task_name
				,(
						select case when "ApprStepYN"='Y' and "State" is null then 'Y' else 'N' end 
				from appr_result 
				where 1=1
				and "SourceDataPk" = r."SourceDataPk"
				and "SourceTableName" = r."SourceTableName"
				and "Line" = 1
				and _status = 'A'
				) as "CollectYN"
			from v_appr_result r
			inner join bundle_head b on r."SourceDataPk" = b.id and r."SourceTableName" in('bundle_head','haccp_diary') 
			left join task_master tm on r."TaskMasterCode" = tm."Code"
			where 1=1
			and r._created between cast(:from_date as date) and cast(:to_date as date)
			and r."State" in ('reprocess', 'process', 'approval', 'reject') 
			and ((:tm_id != '') and (tm.id = :tm_id::Integer) or (:tm_id = '') and (1=1))
			and exists (
				select 1
				from appr_result 
				where r."SourceDataPk" = "SourceDataPk"
				and r."SourceTableName" = "SourceTableName"
				and "Approver_id" = :user_id
				and "State" in ('approval')
				and "Line" > 0
			)
			order by r._created desc
					""";
			
			items = this.sqlRunner.getRows(sql, paramMap);
		}
		
		
		
		
		return items;
	}	

}
