package mes.app.support;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.support.service.MeetingCalendarService;
import mes.domain.entity.Calendar;
import mes.domain.entity.MatOrder;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.CalendarRepository;
import mes.domain.repository.MatOrderRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/support/calendar")
public class MeetingCalendarController {
	
	@Autowired
	private MeetingCalendarService meetingCalendarService;
	
	@Autowired
	CalendarRepository calendarRepository;
	
	@Autowired
	MatOrderRepository matOrderRepository;
	
	@GetMapping("/read")
	public AjaxResult getMeetingCalendar(
			@RequestParam("keyword") String keyword) {
		
		List<Map<String, Object>> items = this.meetingCalendarService.getMeetingCalendar(keyword);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@PostMapping("/save")
	public AjaxResult saveMeetingCalendar(
			@RequestParam(value="_id", required=false) String id,
			@RequestParam("color") String color,
			@RequestParam("datadate") String datadate,
			@RequestParam("description") String description,
			@RequestParam("endTime") String endTime,
			@RequestParam("startTime") String startTime,
			@RequestParam("title") String title,
			@RequestParam("start") String start,
			@RequestParam("end") String end,
			@RequestParam("data_div") String dataDiv,
			@RequestParam("allDay") String allDay,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		Calendar ca = null;
		if (id.contains("mo")) {
			id = id.replace("mo", "");
			MatOrder mo = this.matOrderRepository.getMatOrderById(id);
			mo.setDescription(description);
			mo = this.matOrderRepository.save(mo);
		} else {
			if (id.equals("")) {
				ca = new Calendar();
			} else {
				ca = this.calendarRepository.getCalendarById(Integer.parseInt(id));
			}
			
			Timestamp dataDate = CommonUtil.tryTimestamp(datadate);
			ca.setDataDate(dataDate);
			ca.setStartTime(Time.valueOf(startTime));
			ca.setEndTime(Time.valueOf(endTime));
			ca.setTitle(title);
			ca.setColor(color);
			ca.setDescription(description);
			ca.set_audit(user);
			
			ca = this.calendarRepository.save(ca);
		}
		
		AjaxResult result = new AjaxResult();
        result.data = ca;
		return result;
		
	}
	
	@PostMapping("/delete")
	public AjaxResult deleteCalendar(
			@RequestParam(value="_id", required=false) Integer id) {
		this.calendarRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		return result;
	}
	
}
