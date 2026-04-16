package mes.app.system;

import java.sql.Timestamp;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.system.service.SystemService;
import mes.domain.model.AjaxResult;

@RestController
@RequestMapping("/api/system/systemlog")
public class SystemLogController {
	@Autowired
	SystemService systemService;
	
	@GetMapping("/read")
	public AjaxResult systemLogList(
			@RequestParam(value="srchStartDt", required = false) String start,
			@RequestParam(value="srchEndDt", required = false) String end,
			@RequestParam(value="srchType", required = false) String type,
			@RequestParam(value="srchSource", required = false) String source
	) {

		start = start + " 00:00:00";
		end = end + " 23:59:59";

		Timestamp tsStart = Timestamp.valueOf(start);
		Timestamp tsEnd = Timestamp.valueOf(end);

		AjaxResult result = new AjaxResult();
		result.data = this.systemService.getSystemLogList(tsStart, tsEnd, type, source);
		result.success = true;
		return result;
	}
	
	@GetMapping("/detail")
    public AjaxResult systemLogDetail(
    		@RequestParam(value="log_id") long id    		
    		) {
		
        AjaxResult result = new AjaxResult();
        result.data = this.systemService.getSystemLogDetail(id);
        result.success = true;
		return result;
	}
	
}
