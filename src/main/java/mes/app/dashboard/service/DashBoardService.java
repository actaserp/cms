package mes.app.dashboard.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.security.core.Authentication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.entity.User;
import mes.domain.services.SqlRunner;

@Service
public class DashBoardService {

	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> todayWeekProd(String spjangcd) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("spjangcd", spjangcd);

		String sql = """
				with aa as (
						select * from job_res jr
						-- 금일
						where jr."ProductionDate" = current_date
						and jr."spjangcd" = :spjangcd
						), a1 as (
							-- 지시량
						     select sum(aa."OrderQty") as total_qty from aa where aa."State" !='canceled' 
						), a2 as(
							-- 완료량
						   select sum(aa."GoodQty") as good_qty from aa where aa."State" ='finished'
						), a3 as (
							-- 작업중
							select sum(aa."GoodQty") as working_qty from aa where aa."State" ='working'
						), bb as (
						select * from job_res jr
						-- 전일
						where jr."ProductionDate" = current_date - 1
						and jr."spjangcd" = :spjangcd
						), b1 as (
						     select sum(bb."OrderQty") as total_qty from bb where bb."State" !='canceled' 
						), b2 as(
						   select sum(bb."GoodQty") as good_qty from bb where bb."State" ='finished'
						), b3 as (
							select sum(bb."GoodQty") as working_qty from bb where bb."State" ='working'
						), cc as (
						select * from job_res jr
						-- 금주
						where jr."ProductionDate" between  date_trunc('week', current_date)::date and date_trunc('week', current_date)::date + 6
						and jr."spjangcd" = :spjangcd
						), c1 as (
						     select sum(cc."OrderQty") as total_qty from cc where cc."State" !='canceled' 
						), c2 as(
						   select sum(cc."GoodQty") as good_qty from cc where cc."State" ='finished'
						), c3 as (
							select sum(cc."GoodQty") as working_qty from cc where cc."State" ='working'
						), dd as (
						select * from job_res jr
						-- 전주
						where jr."ProductionDate" between date_trunc('week', current_date - 7)::date and date_trunc('week', current_date - 7)::date + 6
						and jr."spjangcd" = :spjangcd
						), d1 as (
						     select sum(dd."OrderQty") as total_qty from dd where dd."State" !='canceled' 
						), d2 as(
						   select sum(dd."GoodQty") as good_qty from dd where dd."State" ='finished'
						), d3 as (
							select sum(dd."GoodQty") as working_qty from dd where dd."State" ='working'
						)
						select '금일' as type
							, coalesce(a1.total_qty,0) as ord
							, coalesce(a2.good_qty,0) as com
							, coalesce(a3.working_qty,0) as wor
							, (case when a2.good_qty > 0 and a1.total_qty > 0 then trunc((a2.good_qty/a1.total_qty)*100) else 0 end) as wor_per 
						from a1 
						left join a2 on 1=1
						left join a3 on 1=1
						union all 
						select '전일'
						, coalesce(b1.total_qty,0)
						, coalesce(b2.good_qty,0)
						, coalesce(b3.working_qty,0)
						, (case when b2.good_qty > 0 and b1.total_qty > 0 then trunc((b2.good_qty/b1.total_qty)*100) else 0 end)
						from b1
						left join b2 on 1=1
						left join b3 on 1=1
						union all 
						select '금주'
						, coalesce(c1.total_qty,0)
						, coalesce(c2.good_qty,0)
						, coalesce(c3.working_qty,0)
						, (case when c2.good_qty > 0 and c1.total_qty > 0 then trunc((c2.good_qty/c1.total_qty)*100) else 0 end)
						from c1
						left join c2 on 1=1
						left join c3 on 1=1
						union all 
						select '전주'
						, coalesce(d1.total_qty,0)
						, coalesce(d2.good_qty,0)
						, coalesce(d3.working_qty,0)
						, (case when d2.good_qty > 0 and d1.total_qty > 0 then trunc((d2.good_qty/d1.total_qty)*100) else 0 end)
						from d1
						left join d2 on 1=1
						left join d3 on 1=1
					""";
		
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

	public List<Map<String, Object>> todayProd(String spjangcd) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("spjangcd", spjangcd);

		String sql = """
					select m."Name" as prod, mg."Name" as prod_grp
					, sum(case when jr."State" != 'canceled' then jr."OrderQty" else 0 end) as ord
					, sum(case when jr."State" = 'finished' then jr."GoodQty" else 0 end) as com
					, sum(case when jr."State" = 'working' then jr."GoodQty" else 0 end) as wor
					, (case when sum(case when jr."State" != 'canceled' then jr."OrderQty" else 0 end) > 0 
						and sum(case when jr."State" = 'finished' then jr."GoodQty" else 0 end) > 0 then 
						trunc((sum(case when jr."State" = 'finished' then jr."GoodQty" else 0 end)/sum(case when jr."State" != 'canceled' then jr."OrderQty" else 0 end)*100)) else 0 end) as wor_per 
					from job_res jr 
					inner join material m on jr."Material_id"  = m.id
					inner join mat_grp mg on m."MaterialGroup_id"  = mg.id
					where jr."ProductionDate"  = current_date
					and jr."spjangcd" = :spjangcd 
					group by m."Name", mg."Name" 
					order by ord desc
					limit 5
				""";
		
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

	public List<Map<String, Object>> yearDefProd(String spjangcd) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("spjangcd", spjangcd);

		String sql = """
			select mg."Name" as prod_grp , m."Name" as prod , coalesce(m."UnitPrice",0) as unitp
			,coalesce(sum(jrd."DefectQty") * m."UnitPrice",0) as dep
			,coalesce(sum(jrd."DefectQty"),0) as deq
			from job_res_defect jrd 
			left join job_res jr on jrd."JobResponse_id" = jr.id
			left join material m on jr."Material_id"  = m.id
			left join mat_grp mg on m."MaterialGroup_id"  = mg.id
			where to_char(jr."ProductionDate",'YYYY') = to_char(current_date,'YYYY')
			and jr."spjangcd" = :spjangcd
			group by mg."Name" , m."Name" , m."UnitPrice"
			having coalesce(sum(jrd."DefectQty"),0) > 0
			order by coalesce(sum(jr."DefectQty") * m."UnitPrice",0) desc
			limit 5
			""";
	
	List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
	
	return items;
	}

	public List<Map<String, Object>> matStock(String spjangcd) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("spjangcd", spjangcd);

		String sql = """
				select m."Name" as prod ,mg."Name" as prod_grp
				,coalesce(m."UnitPrice",0) as unitp
				,coalesce(sum(m."CurrentStock") * m."UnitPrice",0) as stp
				,coalesce(sum(m."CurrentStock"),0) as stq
				from material m 
				inner join mat_grp mg  on mg.id = m."MaterialGroup_id" 
				where m."CurrentStock" > 0
				and m."spjangcd" = :spjangcd
				group by m."Name" , mg."Name" , m."UnitPrice" 
				order by coalesce(sum(m."CurrentStock") * m."UnitPrice",0) desc
				limit 5
				""";
		
	List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
	
	return items;
	}

	public List<Map<String, Object>> customOrder(String spjangcd) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("spjangcd", spjangcd);

		String sql = """
				select mg."Name" as prod_grp, fn_code_name('mat_type', mg."MaterialType" ) as mat_type_name, m."Name" as prod,m."Code" as prod_code ,coalesce(sum(s."SujuQty") * m."UnitPrice" ,0) as sujup
				from suju s
				inner join material m on m.id = s."Material_id" 
				inner join mat_grp mg on mg.id = m."MaterialGroup_id" 
				where to_char(s."JumunDate", 'YYYY') = to_char(current_date, 'YYYY') 
				and m."spjangcd" = :spjangcd
				group by m."Name" , mg."Name" ,m."UnitPrice", m."Code" , mg."MaterialType"
				having coalesce(sum(s."SujuQty") * m."UnitPrice" ,0) > 0
				order by sujup desc
				limit 10
				""";
		
	List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
	
	return items;
	}

	public Map<String, Object> customServiceStat(String dateType) {
		
		
		String sql = """
				with cte as (
					select * from cust_complain cc
				""";
		
		if (dateType.equals("Year")) {
			sql += " where to_char(cc.\"CheckDate\",'YYYY') = to_char(current_date,'YYYY') ";
		} else if (dateType.equals("Mon")) {
			sql += " where to_char(cc.\"CheckDate\",'YYYY-MM') = to_char(current_date,'YYYY-MM') ";
		}
		
		sql += """
				), total as (
				select 	 coalesce(sum(cte."Qty"),0) as "totalCnt"
						, coalesce(sum(case when cte."CheckState" = '조치중' then cte."Qty" else 0 end),0) as working
						, coalesce(sum(case when cte."CheckState" = '조치완료' then cte."Qty" else 0 end),0) as finish
				from cte
				) 
				select  * from total
				""";
		
		Map<String, Object> total = this.sqlRunner.getRow(sql, null);
		
		sql = """
				with cte as (
					select * from cust_complain cc
				""";
		
		if (dateType.equals("Year")) {
			sql += " where to_char(cc.\"CheckDate\",'YYYY') = to_char(current_date,'YYYY') ";
		} else if (dateType.equals("Mon")) {
			sql += " where to_char(cc.\"CheckDate\",'YYYY-MM') = to_char(current_date,'YYYY-MM') ";
		}
		
		sql += """
				), total as (
				select 	 cte."Type",sum(cte."Qty") as "totalCnt"
				from cte
				group by cte."Type"
				) 
				select  * from total
				""";
		
		List<Map<String, Object>> typeList = this.sqlRunner.getRows(sql, null);
		
		Map<String,Object> items = new HashMap<>();
		
		items.put("total", total);
		items.put("typeList", typeList);
		
		
		return items;
	}

	public List<Map<String, Object>> customServiceStatResult() {
		
		String sql = """
				select 
					cc."Type" as name
				""";
		
		for (int i = 1; i < 13; i++) {
				sql += 	" , sum(case when cast(to_char(cc.\"CheckDate\", 'MM') as integer) = "+ i + "then cc.\"Qty\" else 0 end) as p"+ i + " ";
		}
		
		sql += """
				from cust_complain cc
				where to_char(cc."CheckDate", 'YYYY') = to_char(current_date, 'YYYY') 
				group by cc."Type" 
				""";
		
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, null);
		
		return items;
	}

	public Map<String, Object> haccpReadResult(String year_month, String data_year,String data_month,Authentication auth) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("year_month", year_month);
		paramMap.addValue("data_year", data_year);
		paramMap.addValue("data_month", data_month);
		
		User user = (User)auth.getPrincipal();
		
		Integer userId = user.getId();
		paramMap.addValue("userId", userId);
		
		
		
		String sql = "";
		
		
		
		sql = """
				with u as (
				select u."User_id", g.id as "UserGroup_id",u."Name"
				from user_profile u
				inner join rela_data r on u."User_id" = r."DataPk1" and r."RelationName" = 'auth_user-user_group'
				inner join user_group g on r."DataPk2" = g.id and r."RelationName" = 'auth_user-user_group'
			)
			, ar as (
				select rank() over(partition by "TaskMasterCode" order by "ApprDate" desc) as rn 
				, "ApprDate", "TaskMasterCode", "StateName", "LineName","OriginGui","State"
				from v_appr_result
			)
			, re as(
			select distinct tm.id, sc."Value" as code_group_name,"State",tm."Code" as code, tm."TaskName" as task_name, to_char(ar."ApprDate", 'yyyy-MM-dd') as last_appr_date, ar."StateName" as state_name
				, "OriginGui" as menu_link,ta."User_id",tm."WriterGroup_id"--,tm."User_id"
			from task_master tm
			inner join sys_code sc on tm."GroupCode" = sc."Code" and sc."CodeType" = 'task_group_code'
			left join task_approver ta on tm.id = ta."TaskMaster_id" 
			left join u on tm."WriterGroup_id" = u."UserGroup_id"
			left join ar on tm."Code" = ar."TaskMasterCode" and ar.rn = 1
			where 1=1
			and ta."User_id" =:userId or tm."WriterGroup_id" in (
									select distinct "UserGroup_id"
									from task_approver ta
									inner join user_profile up on ta."User_id" = up."User_id" 
									where ta."User_id" = :userId)
			)		
			select coalesce(sum(case when "State"='process' then 1 else 0 end), 0) as process_count
				, coalesce(sum(case when "State"='approval' then 1 else 0 end), 0) as approval_count
				, coalesce(sum(case when "State"='reject' then 1 else 0 end), 0) as reject_count
			from re				
			""";
		Map<String, Object> appr_list = this.sqlRunner.getRow(sql, paramMap);
		
		
		sql="""
				with u as (
				select u."User_id", g.id as "UserGroup_id",u."Name"
				from user_profile u
				inner join rela_data r on u."User_id" = r."DataPk1" and r."RelationName" = 'auth_user-user_group'
				inner join user_group g on r."DataPk2" = g.id and r."RelationName" = 'auth_user-user_group'
			)
			, ar as (
				select rank() over(partition by "TaskMasterCode" order by "ApprDate" desc) as rn 
				, "ApprDate", "TaskMasterCode", "StateName", "LineName","OriginGui","State"
				from v_appr_result
			)
			select distinct tm.id, sc."Value" as code_group_name,"State",tm."Code" as code, tm."TaskName" as task_name, to_char(ar."ApprDate", 'yyyy-MM-dd') as last_appr_date, ar."StateName" as state_name
				, public.fn_prop_data_char('task_master', tm.id, 'menu_link') as menu_link,ta."User_id",tm."WriterGroup_id"--,tm."User_id"
			from task_master tm
			inner join sys_code sc on tm."GroupCode" = sc."Code" and sc."CodeType" = 'task_group_code'
			left join task_approver ta on tm.id = ta."TaskMaster_id" 
			left join u on tm."WriterGroup_id" = u."UserGroup_id"
			left join ar on tm."Code" = ar."TaskMasterCode" and ar.rn = 1
			where 1=1
			and ta."User_id" =:userId or tm."WriterGroup_id" in (
									select distinct "UserGroup_id"
									from task_approver ta
									inner join user_profile up on ta."User_id" = up."User_id" 
									where ta."User_id" = :userId)
			order by sc."Value", tm."TaskName"
				""";
		
		List<Map<String, Object>> total_busi_list = this.sqlRunner.getRows(sql, paramMap);
		
		
		sql="""
				with u as (
				select u."User_id", g.id as "UserGroup_id", g."Code" as "UserGroup_code"
				from user_profile u
				inner join rela_data r on u."User_id" = r."DataPk1" and r."RelationName" = 'auth_user-user_group'
				inner join user_group g on r."DataPk2" = g.id and r."RelationName" = 'auth_user-user_group'
			)
			, dd as (
				select 'Y' as data_div, min(data_date) as from_date, max(data_date) as to_date 
				from dummy_date 
				where data_year = cast(to_char(now(), 'yyyy') as Integer)
				union all
				select 'H' as data_div, min(data_date) as from_date, max(data_date) as to_date 
				from dummy_date 
				where data_year = cast(to_char(now(), 'yyyy') as Integer)
				group by case when data_month in (1,2,3,4,5,6) then 1 else 2 end
				union all
				select 'Q' as data_div, min(data_date) as from_date, max(data_date) as to_date 
				from dummy_date 
				where data_year = cast(to_char(now(), 'yyyy') as Integer)
				group by case when data_month in (1,2,3) then 1 when data_month in (4,5,6) then 2 when data_month in (7,8,9) then 3 else 4 end
				union all
				select 'M' as data_div, min(data_date) as from_date, max(data_date) as to_date 
				from dummy_date 
				where data_year = cast(to_char(now(), 'yyyy') as Integer)
				group by data_month
				union all
				select 'W' as data_div, min(data_date) as from_date, max(data_date) as to_date 
				from dummy_date 
				where yw_year = cast(to_char(now(), 'yyyy') as Integer)
				group by yw_week
				union all
				select 'D' as data_div, data_date as from_date, data_date as to_date 
				from dummy_date 
				where data_year = cast(to_char(now(), 'yyyy') as Integer)
			)
			, task_t as (
				select sc."Value" as code_group_name, tm."TaskName" as task_name, tm."CycleBase" as cycle_base, pd."Char1" as cycle_check, dd.from_date, dd.to_date
					, count(ar."ApprDate") as write_count
					, case when count(ar."ApprDate")>0 then 'Y' else 'N' end as write_yn
				from task_master tm
				inner join sys_code sc on tm."GroupCode" = sc."Code" and sc."CodeType" = 'task_group_code'
				inner join u on tm."WriterGroup_id" = u."UserGroup_id"
				inner join prop_data pd on tm.id = pd."DataPk" and "TableName" = 'task_master' 
					and (
						(tm."CycleBase" in ('W','M','Y') and pd."Code" in ('cycle_date1'))
						or (tm."CycleBase"='Q' and pd."Code" in ('cycle_date1', 'cycle_date2', 'cycle_date3', 'cycle_date4'))
						or (tm."CycleBase"='H' and pd."Code" in ('cycle_date1', 'cycle_date2'))
					)
				inner join dd on tm."CycleBase" = dd.data_div and now() between cast(dd.from_date as date ) and dd.to_date+(interval '0.99999 DAY')
				left join v_appr_result ar on tm."Code" = ar."TaskMasterCode" and ar."ApprDate" between cast(dd.from_date as date) and cast(dd.to_date as date)+(interval '0.99999 DAY')
				where u."User_id" = :userId
				and public.fn_task_cycle_check(upper(tm."CycleBase"), pd."Char1", now()::date) = 'Y'
				group by sc."Value", tm."TaskName", tm."CycleBase", pd."Char1", dd.from_date, dd.to_date
			)
			select *
			from task_t
			order by code_group_name, task_name
				""";
		
		
		List<Map<String, Object>> today_busi_list = this.sqlRunner.getRows(sql, paramMap);
		
		
		sql = """
				with u as (
				select u."User_id", g.id as "UserGroup_id", g."Code" as "UserGroup_code"
				from user_profile u
				inner join rela_data r on u."User_id" = r."DataPk1" and r."RelationName" = 'auth_user-user_group'
				inner join user_group g on r."DataPk2" = g.id and r."RelationName" = 'auth_user-user_group'
			)
			, task_t as (
				select sc."Value" as code_group_name, tm."TaskName" as task_name, tm."CycleBase" as cycle_base, pd."Char1" as cycle_check--, dd.from_date, dd.to_date
					, dd.data_date
				from task_master tm
				inner join sys_code sc on tm."GroupCode" = sc."Code" and sc."CodeType" = 'task_group_code'
				inner join u on tm."WriterGroup_id" = u."UserGroup_id"
				inner join prop_data pd on tm.id = pd."DataPk" and "TableName" = 'task_master' 
					and (
						(tm."CycleBase" in ('W','M','Y') and pd."Code" in ('cycle_date1'))
						or (tm."CycleBase"='Q' and pd."Code" in ('cycle_date1', 'cycle_date2', 'cycle_date3', 'cycle_date4'))
						or (tm."CycleBase"='H' and pd."Code" in ('cycle_date1', 'cycle_date2'))
					)
				inner join dummy_date dd on dd.data_year=(:data_year::Integer) and dd.data_month=(:data_month::Integer) and public.fn_task_cycle_check(upper(tm."CycleBase"), pd."Char1", dd.data_date) = 'Y'
				where u."User_id" = :userId
			)
			, calib_t as (
				select t."Name" as "Name", t."CycleBase", t."CycleNumber", t."SourceTableName", r."CalibDate"
					,to_char(coalesce(
						case when t."CycleBase" = 'Y' and r."CalibDate" is not null then r."CalibDate" + ("CycleNumber"::text||' year')::interval
							when t."CycleBase" = 'M' and r."CalibDate" is not null then r."CalibDate" + ("CycleNumber"::text||' month')::interval 
							else r."CalibDate"
							end
					,now()),'yyyy-MM-dd') "NextCalibDate"
				from calib_inst t
				inner join u on (
					select ug."Code"
					from user_profile up 
					inner join user_group ug on up."UserGroup_id" = ug.id
					where up."User_id" = :userId) = u."UserGroup_code"
				left join (
					select "CalibInstrument_id", max("CalibDate") as "CalibDate"
					from calib_result
					group by "CalibInstrument_id"
				) r on t.id = r."CalibInstrument_id"
				where u."User_id" = :userId
				and to_char(coalesce(
						case when t."CycleBase" = 'Y' and r."CalibDate" is not null then r."CalibDate" + ("CycleNumber"::text||' year')::interval
							when t."CycleBase" = 'M' and r."CalibDate" is not null then r."CalibDate" + ("CycleNumber"::text||' month')::interval 
							else r."CalibDate"
							end
					,now()),'yyyy-MM')  = :data_year||:data_month
			)
			select *
			from task_t
			union all
			select '검교정관리', "Name"||' 검교정', "CycleBase", "CycleNumber"::text, "NextCalibDate"::date
			from calib_t
			order by code_group_name, task_name
				""";
		
		
		List<Map<String, Object>> calendar_list = this.sqlRunner.getRows(sql, paramMap);
		
		
		
		
		
		Map<String, Object> items = new HashMap<>();
		
		items.put("appr_list", appr_list);
		items.put("total_busi_list", total_busi_list);
		items.put("today_busi_list", today_busi_list);
		items.put("calendar_list", calendar_list);
		
		
		
		return items;
	}

	public Map<String, Object> getCppList(String strDate,Authentication auth) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("strDate",strDate);
		
		String sql = "";
		
		sql = """
				with A as(
		select ht."HaccpDiary_id"
		, hd."HaccpProcess_id"
		, ht.id as ht_id
		,hd."DataDate"
		, ht."DataType"
		, substring( ht."StartTime"::text,1, 5) as "StartTime"
		, ht."EndTime"
		, ht."Material_id"
		, m."Name" as "MaterialName"
		, m."Code" as mat_code
		, ht."Equipment_id"
		, e."Name" as equ_name
		, ht."Judge" , ht."TesterName" , ht."Description"
		, null::integer as hir_id, null::integer as item_id,null as item_name, null as unit_name
		, null::float as "NumResult", null::float as "LowSpec", null::float as "UpperSpec", null as "SpecText"
		, 0 as _order
		from  haccp_test ht 
		inner join haccp_diary hd  on ht."HaccpDiary_id" = hd.id
		inner join haccp_proc hp on hp.id = hd."HaccpProcess_id"
		left join material m on m.id = ht."Material_id"
		left join equ e on e.id= ht."Equipment_id"
		where hd."DataDate" between (:strDate::date) - interval '2 week' and (:strDate::date)
		and hp."Code" = 'CCP-2B_02'
		), B as (
		select null::integer as "HaccpDiary_id"
		, null::integer as "HaccpProcess_id"
		, tt.ht_id
		, tt."DataType"
		, tt."StartTime"
		 ,tt."EndTime"
		, null::integer as "Material_id"
		, null as"MaterialName"
		, null::integer as "Equipment_id"
		, null::text as equ_name
		, null as "Judge", null as "TesterName", null as "Description"
		, hir.id as hir_id, hi.id as item_id, hi."Name" as item_name, u."Name" as unit_name
		, case when hir."NumResult" is null then hir."CharResult"
		when hir."NumResult" is not null then hir."NumResult"::text end as "NumResult"
		, hil."LowSpec" , hil."UpperSpec", hil."SpecText" 
			, hpi._order
		   from A tt
		  --  inner join haccp_test ht on tt."HaccpDiary_id" = ht."HaccpDiary_id" 
		--and ht.id = tt.ht_id
		inner join haccp_proc hp on hp.id = tt."HaccpProcess_id"
		inner join haccp_proc_item hpi on hpi."HaccpProcess_id" = hp.id
		inner join haccp_item hi on hi.id = hpi."HaccpItem_id"
		left join unit u on u.id = hi."Unit_id"
		left join haccp_item_result hir on hir."HaccpTest_id" = tt.ht_id
		and hir."HaccpItem_id"= hpi."HaccpItem_id"
		left join haccp_item_limit hil on hil."HaccpProcess_id" = hp.id  
		and tt."Material_id" = hil."Material_id" 
		and hil."HaccpItem_id" = hpi."HaccpItem_id" 
		where hp."Code"='CCP-2B_02'
		)
		select 1 as t_lvl, "HaccpDiary_id", "HaccpProcess_id","DataDate" , ht_id, "DataType", "StartTime", "EndTime", "Material_id",mat_code, "MaterialName", "Equipment_id", equ_name
		, "Judge", "TesterName", "Description", hir_id, item_id, item_name, unit_name 
		, "NumResult"::text, "LowSpec", "UpperSpec", "SpecText", _order
		from A
		union all
		select 2 as t_lvl, "HaccpDiary_id", "HaccpProcess_id",to_date('','YYYY-MM-DD') as "DataDate" , ht_id, "DataType", "StartTime", "EndTime", "Material_id",'' as mat_code, "MaterialName", "Equipment_id", equ_name
		, "Judge", "TesterName", "Description", hir_id, item_id, item_name, unit_name 
		, "NumResult", "LowSpec", "UpperSpec", "SpecText", _order
		from B 
		order by "StartTime", ht_id, t_lvl, _order;
				""";
		
		List<Map<String, Object>> getCcpRoastList = this.sqlRunner.getRows(sql, paramMap);
		
		
		
		sql = """
				with A as(
		select ht."HaccpDiary_id"
		, hd."HaccpProcess_id"
		, ht.id as ht_id
		, ht."DataType"
		, substring( ht."StartTime"::text,1, 5) as "StartTime"
		, hd."DataDate"
		, ht."EndTime"
		, ht."Material_id"
		, m."Name" as "MaterialName"
		, m."Code" as mat_code
		, ht."Equipment_id"
		, e."Name" as equ_name
		, ht."Judge" , ht."TesterName" , ht."Description"
		, null::integer as hir_id, null::integer as item_id,null as item_name, null as unit_name
		, null::float as "NumResult", null::float as "LowSpec", null::float as "UpperSpec", null as "SpecText"
		, 0 as _order
		from  haccp_test ht 
		inner join haccp_diary hd  on ht."HaccpDiary_id" = hd.id
		inner join haccp_proc hp on hp.id = hd."HaccpProcess_id"
		left join material m on m.id = ht."Material_id"
		left join equ e on e.id= ht."Equipment_id"
		where hd."DataDate" between (:strDate::date) - interval '2 week' and (:strDate::date)
		and hp."Code" = 'CCP-2B_01'
		--where  ht."HaccpDiary_id" = :hd_id
		), B as (
		select null::integer as "HaccpDiary_id"
		, null::integer as "HaccpProcess_id"
		, tt.ht_id
		, tt."DataType"
		, tt."StartTime"
		 ,tt."EndTime"
		, null::integer as "Material_id"
		, null as"MaterialName"
		, null::integer as "Equipment_id"
		, null::text as equ_name
		, null as "Judge", null as "TesterName", null as "Description"
		, hir.id as hir_id, hi.id as item_id, hi."Name" as item_name, u."Name" as unit_name
		, case when hir."NumResult" is null then hir."CharResult"
		when hir."NumResult" is not null then hir."NumResult"::text end as "NumResult"
		, hil."LowSpec" , hil."UpperSpec", hil."SpecText" 
			, hpi._order
		   from A tt
		  --  inner join haccp_test ht on tt."HaccpDiary_id" = ht."HaccpDiary_id" 
		--and ht.id = tt.ht_id
		inner join haccp_proc hp on hp.id = tt."HaccpProcess_id"
		inner join haccp_proc_item hpi on hpi."HaccpProcess_id" = hp.id
		inner join haccp_item hi on hi.id = hpi."HaccpItem_id"
		left join unit u on u.id = hi."Unit_id"
		left join haccp_item_result hir on hir."HaccpTest_id" = tt.ht_id
		and hir."HaccpItem_id"= hpi."HaccpItem_id"
		left join haccp_item_limit hil on hil."HaccpProcess_id" = hp.id  
		and tt."Material_id" = hil."Material_id" 
		and hil."HaccpItem_id" = hpi."HaccpItem_id" 
		where  hp."Code" = 'CCP-2B_01'
		)
		select 1 as t_lvl, "HaccpDiary_id", "HaccpProcess_id","DataDate" , ht_id, "DataType", "StartTime", "EndTime", "Material_id",mat_code, "MaterialName", "Equipment_id", equ_name
		, "Judge", "TesterName", "Description", hir_id, item_id, item_name, unit_name
		, "NumResult"::text, "LowSpec", "UpperSpec", "SpecText", _order
		from A
		union all
		select 2 as t_lvl, "HaccpDiary_id", "HaccpProcess_id",to_date('','YYYY-MM-DD') as "DataDate" , ht_id, "DataType", "StartTime", "EndTime", "Material_id",'' as mat_code, "MaterialName", "Equipment_id", equ_name
		, "Judge", "TesterName", "Description", hir_id, item_id, item_name, unit_name
		, "NumResult", "LowSpec", "UpperSpec", "SpecText", _order
		from B 
		order by "StartTime", ht_id, t_lvl, _order;
				""";
		
		
		List<Map<String, Object>> getccpMbathList = this.sqlRunner.getRows(sql, paramMap);
		
		
		
		
		
		sql = """
				with A as(
		select ht."HaccpDiary_id"
		, hd."HaccpProcess_id"
		, ht.id as ht_id
		, ht."DataType"
		, substring( ht."StartTime"::text,1, 5) as "StartTime"
		, hd."DataDate"
		, ht."EndTime"
		, ht."Material_id"
		, m."Name" as "MaterialName"
		, m."Code" as mat_code
		, ht."Equipment_id"
		, e."Name" as equ_name
		, ht."Judge" , ht."TesterName" , ht."Description"
		, null::integer as hir_id, null::integer as item_id,null as item_name, null as unit_name
		, null::float as "NumResult", null::float as "LowSpec", null::float as "UpperSpec", null as "SpecText"
		, 0 as _order
		from  haccp_test ht 
		inner join haccp_diary hd  on ht."HaccpDiary_id" = hd.id
		inner join haccp_proc hp on hp.id = hd."HaccpProcess_id"
		left join material m on m.id = ht."Material_id"
		left join equ e on e.id= ht."Equipment_id"
		where hd."DataDate" between (:strDate::date) - interval '2 week' and (:strDate::date)
		and hp."Code" = 'CCP-3P'
		--where  ht."HaccpDiary_id" = :hd_id
		), B as (
		select null::integer as "HaccpDiary_id"
		, null::integer as "HaccpProcess_id"
		, tt.ht_id
		, tt."DataType"
		, tt."StartTime"
		 ,tt."EndTime"
		, null::integer as "Material_id"
		, null as"MaterialName"
		, null::integer as "Equipment_id"
		, null::text as equ_name
		, null as "Judge", null as "TesterName", null as "Description"
		, hir.id as hir_id, hi.id as item_id, hi."Name" as item_name, u."Name" as unit_name
		, case when hir."NumResult" is null then hir."CharResult"
		when hir."NumResult" is not null then hir."NumResult"::text end as "NumResult"
		, hil."LowSpec" , hil."UpperSpec", hil."SpecText" 
			, hpi._order
		   from A tt
		  --  inner join haccp_test ht on tt."HaccpDiary_id" = ht."HaccpDiary_id" 
		--and ht.id = tt.ht_id
		inner join haccp_proc hp on hp.id = tt."HaccpProcess_id"
		inner join haccp_proc_item hpi on hpi."HaccpProcess_id" = hp.id
		inner join haccp_item hi on hi.id = hpi."HaccpItem_id"
		left join unit u on u.id = hi."Unit_id"
		left join haccp_item_result hir on hir."HaccpTest_id" = tt.ht_id
		and hir."HaccpItem_id"= hpi."HaccpItem_id"
		left join haccp_item_limit hil on hil."HaccpProcess_id" = hp.id  
		and tt."Material_id" = hil."Material_id" 
		and hil."HaccpItem_id" = hpi."HaccpItem_id" 
		where  hp."Code" = 'CCP-3P'
		)
		select 1 as t_lvl, "HaccpDiary_id", "HaccpProcess_id","DataDate" , ht_id, "DataType", "StartTime", "EndTime", "Material_id",mat_code, "MaterialName", "Equipment_id", equ_name
		, "Judge", "TesterName", "Description", hir_id, item_id, item_name, unit_name
		, "NumResult"::text, "LowSpec", "UpperSpec", "SpecText", _order
		from A
		union all
		select 2 as t_lvl, "HaccpDiary_id", "HaccpProcess_id",to_date('','YYYY-MM-DD') as "DataDate" , ht_id, "DataType", "StartTime", "EndTime", "Material_id",'' as mat_code, "MaterialName", "Equipment_id", equ_name
		, "Judge", "TesterName", "Description", hir_id, item_id, item_name, unit_name
		, "NumResult", "LowSpec", "UpperSpec", "SpecText", _order
		from B 
		order by "StartTime", ht_id, t_lvl, _order;
				""";
		
		
		List<Map<String, Object>> getccpFmatterList = this.sqlRunner.getRows(sql, paramMap);
		
		Map<String, Object> items =new HashMap<>();
		items.put("getCcpRoastList", getCcpRoastList);
		items.put("getccpMbathList", getccpMbathList);
		items.put("getccpFmatterList", getccpFmatterList);
		
		
		return items;
	}

	public Map<String, Object> getDetailHacpPro() {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		
		String sql = "";
		
		sql = """
				select hpi.id as hpi_id
	            , hpi."HaccpProcess_id" as hp_id
	            , hpi."HaccpItem_id" as item_id
	            , hp."Name" as haccp_process_name
	            , hi."Name" as item_name
	            , hi."ResultType"
	            , u."Name" as unit_name
	            , hpi."_order"
	            , to_char(hpi."_created",'YYYY-MM-DD HH24:MI:SS') as "_created"
	            FROM haccp_proc_item hpi 
	            left join haccp_proc hp on hp.id = hpi."HaccpProcess_id" 
	            left join haccp_item hi on hi.id = hpi."HaccpItem_id" 
	            left join unit u on u.id = hi."Unit_id"
	            where hp."Code" = 'CCP-2B_02'
	            order by hpi."_order"   
				""";
		
		 List<Map<String, Object>> getCcpRoastHead = this.sqlRunner.getRows(sql, paramMap);
		 
		 sql = """
					select hpi.id as hpi_id
		            , hpi."HaccpProcess_id" as hp_id
		            , hpi."HaccpItem_id" as item_id
		            , hp."Name" as haccp_process_name
		            , hi."Name" as item_name
		            , hi."ResultType"
		            , u."Name" as unit_name
		            , hpi."_order"
		            , to_char(hpi."_created",'YYYY-MM-DD HH24:MI:SS') as "_created"
		            FROM haccp_proc_item hpi 
		            left join haccp_proc hp on hp.id = hpi."HaccpProcess_id" 
		            left join haccp_item hi on hi.id = hpi."HaccpItem_id" 
		            left join unit u on u.id = hi."Unit_id"
		            where hp."Code" = 'CCP-2B_01'
		            order by hpi."_order"   
					""";
			
			 List<Map<String, Object>> getccpMbathHead = this.sqlRunner.getRows(sql, paramMap);
			 
			 
		 sql = """
					select hpi.id as hpi_id
		            , hpi."HaccpProcess_id" as hp_id
		            , hpi."HaccpItem_id" as item_id
		            , hp."Name" as haccp_process_name
		            , hi."Name" as item_name
		            , hi."ResultType"
		            , u."Name" as unit_name
		            , hpi."_order"
		            , to_char(hpi."_created",'YYYY-MM-DD HH24:MI:SS') as "_created"
		            FROM haccp_proc_item hpi 
		            left join haccp_proc hp on hp.id = hpi."HaccpProcess_id" 
		            left join haccp_item hi on hi.id = hpi."HaccpItem_id" 
		            left join unit u on u.id = hi."Unit_id"
		            where hp."Code" = 'CCP-3P'
		            order by hpi."_order"   
					""";
			
			 List<Map<String, Object>> getFmatterHead = this.sqlRunner.getRows(sql, paramMap);
			 
			 Map<String, Object> items =new HashMap<>();
			 items.put("getCcpRoastHead", getCcpRoastHead);
			 items.put("getccpMbathHead", getccpMbathHead);
			 items.put("getFmatterHead", getFmatterHead);
	        
	     return items;
	}

}
