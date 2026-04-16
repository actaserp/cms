package mes.app.system;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.system.service.LoginLogService;
import mes.domain.model.AjaxResult;

@RestController
@RequestMapping("/api/system/loginlog")
public class LoginLogController {

	@Autowired
	private LoginLogService loginLogService;

	// 로그인 로그 리스트 조회
	@GetMapping("/read")
	public AjaxResult getLoginLogList(
			@RequestParam(value="srchStartDt", required=false) String srchStartDt,
			@RequestParam(value="srchEndDt", required=false) String srchEndDt,
			@RequestParam(value="keyword", required=false) String keyword,
			@RequestParam(value="type", required=false) String type,
			HttpServletRequest request) {
		String start_date = srchStartDt + " 00:00:00";
		String end_date = srchEndDt + " 23:59:59";

		Timestamp start = Timestamp.valueOf(start_date);
		Timestamp end = Timestamp.valueOf(end_date);

		List<Map<String, Object>> items = this.loginLogService.getLoginLogList(start, end, keyword, type);


		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}
}
