package mes.app.production;

import lombok.extern.slf4j.Slf4j;
import mes.app.definition.service.material.UnitPriceService;
import mes.app.production.service.JobPlanService;
import mes.app.sales.service.SujuService;
import mes.app.sales.service.SujuUploadService;
import mes.config.Settings;
import mes.domain.entity.*;
import mes.domain.model.AjaxResult;
import mes.domain.repository.*;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/api/production/job_plan")
public class JobPlanController {

	@Autowired
	JobPlanHeadRepository jobPlanHeadRepository;
	
	@Autowired
	JobPlanService jobPlanService;

	@Autowired
	JobPlanRepository jobPlanRepository;

	@Autowired
	MaterialRepository materialRepository;

	@Autowired
	Settings settings;

	@Autowired
	SqlRunner sqlRunner;


	// 목록 조회
	@GetMapping("/read")
	public AjaxResult getPlanList(
			@RequestParam(value="date_kind", required=false) String date_kind,
			@RequestParam(value="start", required=false) String start_date,
			@RequestParam(value="end", required=false) String end_date,
			@RequestParam(value="spjangcd") String spjangcd,
			HttpServletRequest request) {

		if (start_date != null) start_date = start_date.replaceAll("-", "");
		if (end_date != null) end_date = end_date.replaceAll("-", "");

		List<Map<String, Object>> items = this.jobPlanService.getList(date_kind, start_date, end_date, spjangcd);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}

	// 목록 조회
	@GetMapping("/read_actual_plan")
	public AjaxResult getActualPlanList(
			@RequestParam(value="date_kind", required=false) String date_kind,
			@RequestParam(value="start", required=false) String start_date,
			@RequestParam(value="end", required=false) String end_date,
			@RequestParam(value="spjangcd") String spjangcd,
			HttpServletRequest request) {

		if (start_date != null) start_date = start_date.replaceAll("-", "");
		if (end_date != null) end_date = end_date.replaceAll("-", "");

		Map<String, Object> items = this.jobPlanService.getActualPlanList(date_kind, start_date, end_date, spjangcd);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}
	
	// 상세정보 조회
	@GetMapping("/detail")
	public AjaxResult getPlanDetail(
			@RequestParam("head_id") int head_id,
			HttpServletRequest request) {
		Map<String, Object> item = this.jobPlanService.getDetail(head_id);
		
		AjaxResult result = new AjaxResult();
		result.data = item;
		
		return result;
	}

	// 등록
	@PostMapping("/save")
	@Transactional
	public AjaxResult PlanSave(@RequestBody Map<String, Object> payload, Authentication auth) {
		User user = (User) auth.getPrincipal();
		AjaxResult result = new AjaxResult();

		try {
			// 1. 공통값 파싱
			Object headIdRaw = payload.get("head_id");
			Long headId = null;

			if (headIdRaw != null) {
				String headIdStr = headIdRaw.toString().trim();
				if (!headIdStr.isEmpty()) {
					headId = Long.parseLong(headIdStr);
				}
			}
			String actnm = (String) payload.get("actnm");
			String stdate = ((String) payload.get("stdate")).replaceAll("-", "");
			String eddate = ((String) payload.get("eddate")).replaceAll("-", "");
			String datetype = (String) payload.get("datetype");
			String cmcode = (String) payload.get("cmcode");
			String spjangcd = (String) payload.get("spjangcd");
			String description = (String) payload.get("description");

			// 2. 헤더 저장 (신규 or 수정)
			JobPlanHead head;
			if (headId != null) {
				head = jobPlanHeadRepository.findById(headId).orElseThrow(() -> new IllegalArgumentException("헤더를 찾을 수 없습니다."));
			} else {
				head = new JobPlanHead();
			}

			head.setActnm(actnm);
			head.setStdate(stdate);
			head.setEddate(eddate);
			head.setDatetype(datetype);
			head.setCmcode(cmcode);
			head.setSpjangcd(spjangcd);
			head.setDescription(description);
			jobPlanHeadRepository.save(head);

			// 3. 기존 아이템 모두 삭제 후 재삽입
			jobPlanRepository.deleteByHead_Id(head.getId());

			List<Map<String, Object>> items = (List<Map<String, Object>>) payload.get("items");

			for (Map<String, Object> itemMap : items) {
				JobPlan item = new JobPlan();
				item.setHead(head);
				item.setMaterial_id(toInteger(itemMap.get("material_id")));
				item.setQty(toInteger(itemMap.get("qty")));
				item.setRemark((String) itemMap.get("remark"));
				item.setSpjangcd(spjangcd); // 헤더 기준 동일 값 사용
				jobPlanRepository.save(item);
			}

			result.success = true;
			result.data = Map.of("head_id", head.getId());

		} catch (Exception e) {
			result.success = false;
			result.message = e.getMessage();
		}

		return result;
	}

	private Integer toInteger(Object value) {
		if (value == null) return null;
		try {
			return Integer.parseInt(value.toString());
		} catch (Exception e) {
			return null;
		}
	}


	// 삭제
	@Transactional
	@PostMapping("/delete")
	public AjaxResult deletePlan(
			@RequestParam("id") Integer head_id) {
		
		AjaxResult result = new AjaxResult();

		jobPlanRepository.deleteByHead_Id(Long.valueOf(head_id));

		// 2. 그 다음 job_plan_head 삭제
		jobPlanHeadRepository.deleteById(Long.valueOf(head_id));
		
		return result;
	}

}
