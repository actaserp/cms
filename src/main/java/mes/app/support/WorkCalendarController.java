package mes.app.support;

import java.sql.Date;
import java.sql.Time;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.support.service.WorkCalendarService;
import mes.domain.entity.User;
import mes.domain.entity.WorkCalendar;
import mes.domain.model.AjaxResult;
import mes.domain.repository.WorkCalendarRepository;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/support/work_calendar")
public class WorkCalendarController {

	@Autowired
	private WorkCalendarService workCalendarService;
	
	@Autowired
	WorkCalendarRepository workCalendarRepository;
	
	@Autowired
	SqlRunner sqlRunner;
	
	@GetMapping("/read")
	private AjaxResult getWorkCalendar(
				@RequestParam(value="data_month", required=false) String dataMonth, 
	    		@RequestParam(value="factory_pk", required=false) String factoryPk
			) {
		
		List<Map<String, Object>> items = this.workCalendarService.getWorkCalendar(dataMonth,factoryPk);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@PostMapping("/save")
	public AjaxResult saveWorkCalendar(
			@RequestParam(value="id", required=false) Integer id, 
    		@RequestParam(value="data_date", required=false) String dataDate,
    		@RequestParam(value="holiday_yn", required=false) String holidayYn, 
    		@RequestParam(value="start", required=false) String start,
    		@RequestParam(value="end", required=false) String end,
    		@RequestParam(value="factory_pk", required=false) String factoryPk,
    		@RequestParam(value="worktime", required=false) String worktime,
    		Authentication auth,
    		HttpServletRequest request) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();

		WorkCalendar wc = new WorkCalendar();
		
		if (id != null) {
			wc = this.workCalendarRepository.getWorkCalendarById(id);
		}
		
		wc.setDataDate(Date.valueOf(dataDate));
		wc.setHolidayYN(holidayYn);
		wc.setWorkHr(Float.parseFloat(worktime));
		wc.setStartTime(Time.valueOf(start + ":00"));
		wc.setEndTime(Time.valueOf(end+ ":00"));
		wc.setDataPk(Integer.parseInt(factoryPk));
		wc.setTableName("factory");
		wc.set_audit(user);
		
		wc = this.workCalendarRepository.save(wc);
		
		return result;
	}
	
	@PostMapping("/multi_save")
	public AjaxResult multiSaveWorkCalendar(
			@RequestParam(value="id", required=false) Integer id, 
    		@RequestParam(value="date_from", required=false) String dateFrom,
    		@RequestParam(value="date_to", required=false) String dateTo,
    		@RequestParam(value="holiday_yn", required=false) String holidayYn, 
    		@RequestParam(value="start", required=false) String start,
    		@RequestParam(value="end", required=false) String end,
    		@RequestParam(value="factory_pk", required=false) String factoryPk,
    		@RequestParam(value="worktime", required=false) String worktime,
    		@RequestParam(value="weekdays", required=false) String weekdays,
    		Authentication auth,
    		HttpServletRequest request) {
	
		User user = (User)auth.getPrincipal();
		
		Integer userId = user.getId();
		
		AjaxResult result = new AjaxResult();

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("weekdays", weekdays);
		paramMap.addValue("dateFrom", dateFrom);
		paramMap.addValue("dateTo", dateTo);
		paramMap.addValue("factoryPk", factoryPk);
		
		String sql = """
                with A as
                (
                    select generate_series( cast(:dateFrom as date) , cast(:dateTo as date), '1 day'::interval)::date as data_date
                ), B as
                (
   	                select unnest(string_to_array(cast(:weekdays as text),','))::integer as wday_index
                )
                delete 
                from work_calendar wc
                using A, B 
                where extract (dow from data_date) = B.wday_index
                and wc."DataDate" = A.data_date
                and wc."DataPk" = (:factoryPk)
				""";
		
		this.sqlRunner.execute(sql, paramMap);
		
		MapSqlParameterSource paramMap2 = new MapSqlParameterSource();
		paramMap2.addValue("holidayYn", holidayYn);
		paramMap2.addValue("userId", userId);
		paramMap2.addValue("factoryPk", factoryPk);
		paramMap2.addValue("dateFrom", dateFrom);
		paramMap2.addValue("dateTo", dateTo);
		
        if("Y".equals(holidayYn)) {
        	paramMap2.addValue("start", null);
        	paramMap2.addValue("end", null);
        	paramMap2.addValue("worktime", 0);
        } else {
        	paramMap2.addValue("start", Time.valueOf(start + ":00"));
        	paramMap2.addValue("end", Time.valueOf(end+ ":00"));
        	paramMap2.addValue("worktime", Float.parseFloat(worktime));
        }
        paramMap2.addValue("weekdays", weekdays);
        
		sql = """
				with A as
                (
                    select generate_series( cast(:dateFrom as date) , cast(:dateTo as date), '1 day'::interval)::date as data_date
                ), B as
                (
   	                select unnest(string_to_array(cast(:weekdays as text),','))::integer as wday_index
                )
                insert into work_calendar("DataPk", "TableName", "DataDate", "HolidayYN"
                , "StartTime", "EndTime", "WorkHr"
                , "_created", "_creater_id", "_modified", "_modifier_id")             
                select cast(:factoryPk as Integer) as data_pk, 'factory', data_date, :holidayYn
	                , :start as start_time
                    , :end as end_time
	                , :worktime as work_hr
	                ,now()
	                ,:userId
	                ,now()
	                ,:userId
                from A 
                inner join B on extract (dow from data_date) = B.wday_index
			  """;
		
		this.sqlRunner.execute(sql, paramMap2);
		
		return result;
	}
	
	@PostMapping("/delete")
	public AjaxResult deleteWorkCalendar(
			@RequestParam(value="id", required=false) Integer id,
			HttpServletRequest request) {
		
		AjaxResult result = new AjaxResult();
		
		this.workCalendarRepository.deleteById(id);
		
		result.success = true;
		
		return result;
	}
}
