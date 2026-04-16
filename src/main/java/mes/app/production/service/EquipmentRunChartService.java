package mes.app.production.service;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@Service
public class EquipmentRunChartService {
	
	@Autowired
	SqlRunner sqlRunner;

	// // 차트 searchMainData
	public List<Map<String, Object>> getEquipmentRunChart(String date_from, String date_to, Integer id, String runType,
														  String spjangcd) {

		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		LocalDateTime start_date = LocalDate.parse(date_from, formatter).atTime(0, 0, 0);
		LocalDateTime end_date = LocalDate.parse(date_to, formatter).atTime(23, 59, 59);



		MapSqlParameterSource dicParam = new MapSqlParameterSource();

        dicParam.addValue("id", id);
        dicParam.addValue("runType", runType);
		dicParam.addValue("date_from", start_date);
		dicParam.addValue("date_to", end_date);
		dicParam.addValue("spjangcd", spjangcd);

		String sql = """
				select
				er."id" as id,
				er."RunState",
				er."StartDate" as "StartDate",
				er."EndDate" as "EndDate",
				to_char(er."StartDate", 'yyyy-mm-dd') as start_date,
				to_char(er."EndDate", 'yyyy-mm-dd') as end_date,
				to_char(er."StartDate", 'HH24:MI') as "StartTime",
				to_char(er."EndDate", 'HH24:MI') as "EndTime",
				er."StopCause_id" as "StopCause_id",
				sc."StopCauseName" as "StopCauseName",
				er."Equipment_id" as "Equipment_id",
				e."Name" as "Name",
				er."Description" as "Description"
				from equ_run er
				left join stop_cause sc on sc.id = er."StopCause_id"
				left join equ e on e.id = er."Equipment_id"
				where er.spjangcd = :spjangcd
				AND (
					(er."StartDate" <= :date_to) AND
					(er."EndDate" IS NULL OR er."EndDate" >= :date_from)
				)
				order by "Name", er."StartDate", er."EndDate"
				""";

        /*String sql = """
        		select er.id
                , to_char(er."StartDate", 'yyyy-mm-dd') as start_date
                , to_char(er."EndDate", 'yyyy-mm-dd') as end_date
	            , e."Name"
	            , e."Code"
	            , er."StartDate"
	            , to_char(er."StartDate",'HH24:MI') as "StartTime"
	            , er."EndDate"
	            , to_char(er."EndDate",'HH24:MI') as "EndTime"
	            , EXTRACT(day from (er."EndDate" - er."StartDate")) * 60 * 24
	                + EXTRACT(hour from (er."EndDate" - er."StartDate")) * 60
	                + EXTRACT(min from ("EndDate" - "StartDate")) as "Runtime"
                , er."WorkOrderNumber"
	            , er."Equipment_id"
	            , er."RunState"
                , sc."StopCauseName"
                , er."Description"
                , er."StopCause_id"
                from equ_grp eg
                inner join equ e on eg.id = e."EquipmentGroup_id"
                left join equ_run er on e.id = er."Equipment_id"
                left join stop_cause sc on sc.id = er."StopCause_id"
                where 1=1
	            --and er."RunState" = :runType
	            """;*/

        
        /*if (id != null) {
        	sql += " and er.id = :id ";
		}else {
	        dicParam.addValue("date_from", CommonUtil.tryTimestamp(date_from));   
	        dicParam.addValue("date_to", Timestamp.valueOf(date_to + " 23:59:59"));
			
			sql += """
	        		AND (
	        			(er."StartDate" <= :date_to) AND
	        			(er."EndDate" IS NULL OR er."EndDate" >= :date_from)
	        		)
     """;
		}
		
		sql += " order by e.\"Name\", er.\"StartDate\", er.\"EndDate\"";
  			    
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);*/

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);


        return items;
	}

	public List<Map<String, Object>> OverlappingTimeQuery(Timestamp startDate, Timestamp endDate, Integer equipmentId, String spjangcd) {



		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("spjangcd", spjangcd);
		dicParam.addValue("EndDate", endDate);
		dicParam.addValue("StartDate", startDate);
		dicParam.addValue("Equipment_id", equipmentId);

		String sql = """
				 select * from equ_run where spjangcd = 'ZZ'
								   AND (
				        (:StartDate < COALESCE("EndDate", now())) AND
				        (:EndDate::timestamp IS NULL OR "StartDate" < :EndDate)
				      )
					  AND "Equipment_id" = :Equipment_id
				""";

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);


		return items;
	}
}
