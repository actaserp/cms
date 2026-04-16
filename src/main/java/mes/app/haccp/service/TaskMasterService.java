package mes.app.haccp.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class TaskMasterService {

	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getCheckResult(String keyword) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("keyword", keyword);  
		
		String sql = """
			with APPROVER as(
				select ta."TaskMaster_id"
                ,sum(case when ta."Line" = 1 and ta."Shift" = 'D' then 1 end) as line1_d_cnt
                ,sum(case when ta."Line" = 1 and ta."Shift" = 'N' then 1 end) as line1_n_cnt
                ,sum(case when ta."Line" = 1 and (ta."Shift" = '' or ta."Shift" is null) then 1 end) as line1_cnt
                ,sum(case when ta."Line" = 2 and ta."Shift" = 'D' then 1 end) as line2_d_cnt
                ,sum(case when ta."Line" = 2 and ta."Shift" = 'N' then 1 end) as line2_n_cnt
                ,sum(case when ta."Line" = 2 and (ta."Shift" = '' or ta."Shift" is null) then 1 end) as line2_cnt
                ,sum(case when ta."Line" = 3 and ta."Shift" = 'D' then 1 end) as line3_d_cnt
                ,sum(case when ta."Line" = 3 and ta."Shift" = 'N' then 1 end) as line3_n_cnt
                ,sum(case when ta."Line" = 3 and (ta."Shift" = '' or ta."Shift" is null) then 1 end) as line3_cnt
                ,sum(case when ta."Line" = 4 and ta."Shift" = 'D' then 1 end) as line4_d_cnt
                ,sum(case when ta."Line" = 4 and ta."Shift" = 'N' then 1 end) as line4_n_cnt
                ,sum(case when ta."Line" = 4 and (ta."Shift" = '' or ta."Shift" is null) then 1 end) as line4_cnt
                from task_approver ta
                inner join user_profile up on up."User_id" = ta."User_id" 
                group by ta."TaskMaster_id"
			)
			select tm.id, tm."GroupCode" as task_group_code, fn_code_name('task_group_code', tm."GroupCode") as task_group_name
	        , tm."Code" as code, tm."TaskName" as task_name, tm."Description" as description
	        , tm."Line1Name" as line1_name 
	        , tm."Line2Name" as line2_name
	        , tm."Line3Name" as line3_name
	        , tm."Line4Name" as line4_name
	        --, case when coalesce(tm."CycleBase",'X') not in ('X') then concat(fn_code_name('cycle_base', tm."CycleBase"),' ', tm."CycleNumber",'회') end as cycle_name 
	        , case when coalesce(tm."CycleBase",'X') not in ('', 'X') then public.fn_code_name('cycle_base', tm."CycleBase") else '수시' end as cycle_name 
            , case when public.fn_code_name('cycle_base', tm."CycleBase")='주' then (
		       case when pd."Char1" ='0' then '토요일' when pd."Char1" ='1' then '일요일' when pd."Char1" ='2' then '월요일' when pd."Char1" ='3' then '화요일'
            when pd."Char1" ='4' then '수요일' when pd."Char1" ='5' then '목요일' when pd."Char1" ='6' then '금요일' end
            ) else null end as cycle_weekday
	        , tm."NotificationYN" as noti_yn
	        , tm."NotificationBefore" as noti_before
	        , tm."LastWriteDate" as last_write_date
	        , tm."NextWriteDate" as next_write_date 
	        , tm."NotificationPlanDate" as noti_plan_date 
            , concat((case when up.line1_d_cnt is not null then concat('주간 ',up.line1_d_cnt,'명') else '' end)
             , (case when up.line1_d_cnt is not null and up.line1_n_cnt is not null then ',' else '' end)
             , (case when up.line1_n_cnt is not null then concat('야간 ',up.line1_n_cnt,'명') else '' end)
             , (case when up.line1_d_cnt is not null and up.line1_n_cnt is not null and up.line1_cnt is not null then ',' else '' end) 
             , (case when up.line1_cnt is not null then concat(up.line1_cnt,'명') else '' end)) as approver1_name
            , concat((case when up.line2_d_cnt is not null then concat('주간 ',up.line2_d_cnt,'명') else '' end)
             , (case when up.line2_d_cnt is not null and up.line2_n_cnt is not null then ',' else '' end)
             , (case when up.line2_n_cnt is not null then concat('야간 ',up.line2_n_cnt,'명') else '' end)
             , (case when up.line2_d_cnt is not null and up.line2_n_cnt is not null and up.line2_cnt is not null then ',' else '' end) 
             , (case when up.line2_cnt is not null then concat(up.line2_cnt,'명') else '' end)) as approver2_name
            , concat((case when up.line3_d_cnt is not null then concat('주간 ',up.line3_d_cnt,'명') else '' end)
             , (case when up.line3_d_cnt is not null and up.line3_n_cnt is not null then ',' else '' end)
             , (case when up.line3_n_cnt is not null then concat('야간 ',up.line3_n_cnt,'명') else '' end)
             , (case when up.line3_d_cnt is not null and up.line3_n_cnt is not null and up.line3_cnt is not null then ',' else '' end) 
             , (case when up.line3_cnt is not null then concat(up.line3_cnt,'명') else '' end)) as approver3_name
            , concat((case when up.line4_d_cnt is not null then concat('주간 ',up.line4_d_cnt,'명') else '' end)
             , (case when up.line4_d_cnt is not null and up.line4_n_cnt is not null then ',' else '' end)
             , (case when up.line4_n_cnt is not null then concat('야간 ',up.line4_n_cnt,'명') else '' end)
             , (case when up.line4_d_cnt is not null and up.line4_n_cnt is not null and up.line4_cnt is not null then ',' else '' end) 
             , (case when up.line4_cnt is not null then concat(up.line4_cnt,'명') else '' end)) as approver4_name
		     , tm."WriterGroup_id" as writer_group_id
		     , g."Name" as writer_group_name
		     , mi."MenuName" as menu_link_name
	        from task_master tm 
	         left join APPROVER up on up."TaskMaster_id" = tm.id
	      left join user_group g on tm."WriterGroup_id" = g.id
            left join menu_item mi on public.fn_prop_data_char('task_master', tm.id, 'menu_link') = mi."MenuCode"
            left join prop_data pd on pd."DataPk" = tm.id and pd."TableName" = 'task_master' and pd."Code" in ('cycle_date1', 'cycle_date2', 'cycle_date3', 'cycle_date4')
	        where 1 = 1
            and tm."TaskName" like concat('%%', :keyword, '%%')
            order by tm."TaskName" 
				""";
		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}

	public Map<String,Object> getTaskMasterDetail(int id) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("id", id);  
		
		//업무기본정보
		String sql = """
			select tm.id, tm."GroupCode" as task_group_code
	        , tm."Code" as code, tm."TaskName" as task_name, tm."Description" as description
	        , tm."Line1Name" as line1_name
            , tm."Line2Name" as line2_name
            , tm."Line3Name" as line3_name
            , tm."Line4Name" as line4_name
            , tm."CycleBase" as cycle_base, tm."CycleNumber" as cycle_number
	        , tm."NotificationYN" as noti_yn
	        , tm."NotificationBefore" as noti_before
	        , tm."LastWriteDate" as last_write_date
	        , tm."NextWriteDate" as next_write_date 
	        , tm."NotificationPlanDate" as noti_plan_date
            , tm."WriterGroup_id" as writer_group
            ,public.fn_prop_data_char('task_master', tm.id, 'menu_link') as menu_link
	        from task_master tm
	        where tm.id = :id
			""";
		
		Map<String, Object> tastk_info = this.sqlRunner.getRow(sql, paramMap);
		
		//업무기본정보
		sql = """
			select ta."User_id"
            , up."Name" as "Name"
            , ta."Depart_id"
	        , d."Name" as "DepartName"
            , ta."Shift"
            , case ta."Shift" when 'D' then '주간' when 'N' then '야간' end as "ShiftName"
            from task_approver ta
            inner join user_profile up on up."User_id" = ta."User_id" 
            left join depart d on d.id = ta."Depart_id"
            where ta."TaskMaster_id" = :id 
            and ta."Line" = :line_num
			""";
		paramMap.addValue("line_num", 1);
		List<Map<String, Object>> approver1_list = this.sqlRunner.getRows(sql, paramMap);
		
		paramMap.addValue("line_num", 2);
		List<Map<String, Object>> approver2_list = this.sqlRunner.getRows(sql, paramMap);
        
		paramMap.addValue("line_num", 3);
		List<Map<String, Object>> approver3_list = this.sqlRunner.getRows(sql, paramMap);
		
		paramMap.addValue("line_num", 4);
		List<Map<String, Object>> approver4_list = this.sqlRunner.getRows(sql, paramMap);
		
		
		sql = """
				select "Code", "Char1" as "Value"
                from prop_data
                where 1=1 
                and "DataPk" = :id 
                and "TableName" = 'task_master'
                and "Code" in ('cycle_date1', 'cycle_date2', 'cycle_date3', 'cycle_date4')
                order by "Code"
				""";
		
		List<Map<String, Object>> cycle_data_list = this.sqlRunner.getRows(sql, paramMap);
		
		
		Map<String,Object> items = new HashMap<String,Object>();
		items.put("task_info", tastk_info);
		items.put("approver1_list", approver1_list);
		items.put("approver2_list", approver2_list);
		items.put("approver3_list", approver3_list);
		items.put("approver4_list", approver4_list);
		items.put("cycle_data_list", cycle_data_list);
		
        return items;
	}

}
