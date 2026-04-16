package mes.app.report;

import lombok.extern.slf4j.Slf4j;
import mes.app.report.service.WorkReportService;
import mes.config.Settings;
import mes.domain.entity.TbAs020;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.WorkReportRepository;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/report/work_report")
public class WorkReportController {

	@Autowired
	WorkReportRepository workreportRepository;
	
	@Autowired
	WorkReportService workReportService;

	@Autowired
	Settings settings;

	@Autowired
	SqlRunner sqlRunner;


	// 목록 조회
	@GetMapping("/read")
	public AjaxResult getReportList(
			@RequestParam(value="start", required=false) String start_date,
			@RequestParam(value="end", required=false) String end_date,
			@RequestParam(value="txtName", required=false) String txtName,
			@RequestParam(value="spjangcd") String spjangcd,
			HttpServletRequest request) {

		if (start_date != null) start_date = start_date.replaceAll("-", "");
		if (end_date != null) end_date = end_date.replaceAll("-", "");

		List<Map<String, Object>> items = this.workReportService.getList(start_date, end_date, txtName, spjangcd);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	// 상세정보 조회
	@GetMapping("/detail")
	public AjaxResult getReportDetail(
			@RequestParam("id") Integer id,
			HttpServletRequest request) {
		Map<String, Object> item = this.workReportService.getDetail(id);
		
		AjaxResult result = new AjaxResult();
		result.data = item;
		
		return result;
	}

	// 등록
	@PostMapping("/save")
	@Transactional
	public AjaxResult ReportSave(@RequestBody Map<String, Object> payload, Authentication auth) {
		User user = (User) auth.getPrincipal();
		AjaxResult result = new AjaxResult();

		try {
			// id 체크
			Integer id = payload.get("id") != null && !payload.get("id").toString().isEmpty()
					? Integer.parseInt(payload.get("id").toString())
					: null;

			TbAs020 entity = null;

			// 수정 모드
			if (id != null) {
				entity = workreportRepository.findById(id)
						.orElseThrow(() -> new RuntimeException("데이터를 찾을 수 없습니다."));
			}
			// 신규 등록 모드
			else {
				entity = new TbAs020();
				entity.setInputdate(new Timestamp(System.currentTimeMillis()));
				entity.setFixperid(String.valueOf(user.getId()));
				entity.setFixpernm(user.getUsername());
			}

			// 공통 필드 매핑
			entity.setRptdate(cleanDate(payload.get("rptdate")));
			entity.setRptweek((String) payload.get("popupWeekSelector"));
			entity.setFrdate(cleanDate(payload.get("stdate")));
			entity.setTodate(cleanDate(payload.get("eddate")));
			entity.setFixperid(String.valueOf(payload.get("fixperid")));
			entity.setFixpernm((String) payload.get("fixpernm"));
			entity.setCltnm((String) payload.get("cltnm"));
			entity.setFixflag((String) payload.get("fixflag"));
			entity.setAsmenu((String) payload.get("asmenu"));
			entity.setActflag((String) payload.get("actflag"));
			entity.setAsdv((String) payload.get("asdv"));
			entity.setRecyn((String) payload.get("recyn"));
			entity.setRptremark((String) payload.get("rptremark"));
			entity.setEtcremark((String) payload.get("etcremark"));
			entity.setRemark((String) payload.get("remark"));

			// 수정자 기록 (등록 시에도 들어가도 무방함)
			entity.setInputdate(new Timestamp(System.currentTimeMillis()));

			workreportRepository.save(entity);

			result.success = true;
			result.data = entity.getRptid();

		} catch (Exception e) {
			e.printStackTrace();
			result.success = false;
			result.message = e.getMessage();
		}

		return result;
	}

	private String cleanDate(Object v) {
		if (v == null) return null;
		return v.toString().replaceAll("-", "");
	}


	// 삭제
	@Transactional
	@PostMapping("/delete")
	public AjaxResult deletePlan(
			@RequestParam("id") Integer id) {
		
		AjaxResult result = new AjaxResult();

		// 2. 그 다음 job_plan_head 삭제
		workreportRepository.deleteById(id);
		
		return result;
	}

}
