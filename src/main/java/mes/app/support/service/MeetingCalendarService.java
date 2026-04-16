package mes.app.support.service;

import java.util.List;
import java.util.Map;

import mes.app.common.TenantContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class MeetingCalendarService {

	@Autowired
	SqlRunner sqlRunner;

	public List<Map<String, Object>> getMeetingCalendar(String keyword) {
		String tenantId = TenantContext.get();
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		String dateFrom = keyword + "-01";
		paramMap.addValue("dateFrom", dateFrom);
		paramMap.addValue("spjangcd", tenantId);

		String sql = """
				select id::text
                , "Title" as title
                , to_char("DataDate",'yyyy-mm-dd') as "DataDate"
                , to_char("DataDate",'yyyy-mm-dd')||' '||to_char("StartTime",'hh24:mi') as start
                , to_char("DataDate",'yyyy-mm-dd')||' '||to_char("EndTime",'hh24:mi') as end
                , "Color" as color
                , 'calendar' as data_div
                , "Description" as description
                from calendar
                where "DataDate" between cast(:dateFrom as date) and cast(:dateFrom as date) + interval '1 month - 1 day'
                and spjangcd = :spjangcd
                union all
                SELECT 'mo'|| mo.id::text
                --, true as allDay
		        --, mo."OrderNumber"
		        --, mo."AvailableStock"
		        , concat(m."Name",' ',mo."OrderQty", u."Name") as title
                , to_char(mo."InputPlanDate",'yyyy-mm-dd') as "DataDate"
                , to_char(mo."InputPlanDate",'yyyy-mm-dd')||' 00:00' as start
                , to_char(mo."InputPlanDate",'yyyy-mm-dd')||' 00:00' as end
	            , '#FF0000' as color
                , 'mat_order' as data_div
                , mo."Description" as description
	            from mat_order mo
	            inner join material m ON m.id = mo."Material_id"
                LEFT JOIN unit u ON m."Unit_id" = u.id
                WHERE mo."InputPlanDate" between cast(:dateFrom as date) and cast(:dateFrom as date) + interval '1 month - 1 day'
                and mo."State" = 'approved' 
				""";
		
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}
}
