package mes.app.haccp;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.transaction.Transactional;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.common.service.FileService;
import mes.app.haccp.service.EduResultService;
import mes.config.Settings;
import mes.domain.entity.BundleHead;
import mes.domain.entity.EduResult;
import mes.domain.entity.EduResultStudent;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BundleHeadRepository;
import mes.domain.repository.EduResultRepository;
import mes.domain.repository.EduResultStudentRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/haccp/edu_result")
public class EduResultController {

	@Autowired
	private EduResultService eduResultService;
	
	@Autowired
	EduResultRepository eduResultRepository;

	@Autowired
	private FileService fileService;
	
	@Autowired
	BundleHeadRepository bundleHeadRepository;
	
	@Autowired
	EduResultStudentRepository eduResultStudentRepository;
	
	@Autowired
	Settings settings;
	
	@GetMapping("/read")
	private AjaxResult getEduResult(
			@RequestParam("date_from") String dateFrom,
			@RequestParam("date_to") String dateTo) {
		
		List<Map<String, Object>> items = this.eduResultService.getEduResult(dateFrom, dateTo);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/detail")
	private AjaxResult getEduResultDetail(
			@RequestParam(value = "id", required = false) Integer id) {
		
		Map<String, Object> items = this.eduResultService.getEduResultDetail(id);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@PostMapping("/save")
	private AjaxResult saveEduResult(
			@RequestParam(value = "id", required = false) Integer id,
			//@RequestParam(value = "fileId", required = false) Integer fileId,
			@RequestParam(value = "fileId", required = false) String fileId,
			@RequestParam(value = "Teacher", required = false) String teacher,
			@RequestParam(value = "TargetCount", required = false) Integer targetCount,
			@RequestParam(value = "StudentCount", required = false) Integer studentCount,
			@RequestParam(value = "StartTime", required = false) String startTime,
			@RequestParam(value = "EndTime", required = false) String endTime,
			@RequestParam(value = "EduTitle", required = false) String eduTitle,
			@RequestParam(value = "EduTarget", required = false) String eduTarget,
			@RequestParam(value = "EduPlace", required = false) String eduPlace,
			@RequestParam(value = "EduMaterial", required = false) String eduMaterial,
			@RequestParam(value = "EduHour", required = false) Integer eduHour,
			@RequestParam(value = "EduEvaluation", required = false) String eduEvaluation,
			@RequestParam(value = "EduDate", required = false) String eduDate,
			@RequestParam(value = "EduContent", required = false) String eduContent,
			@RequestParam(value = "AbsenteeProcess", required = false) String absenteeProcess,
			@RequestParam(value = "SourceDataPk", required = false) Integer SourceDataPk,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
		EduResult er = null;
		
		if ( id == null) {
			er = new EduResult();
			er.setSourceDataPk(SourceDataPk != null ? SourceDataPk : null);
			er.setSourceTableName("edu_year_month_plan");
		} else {
			er = this.eduResultRepository.getEduResultById(id);
		}
		er.setEduDate(Date.valueOf(eduDate));
		er.setEduTitle(eduTitle);
		er.setEduPlace(eduPlace);
		er.setTeacher(teacher);
		er.setEduHour(eduHour);
		er.setStartTime(startTime);
		er.setEndTime(endTime);
		er.setEduTarget(eduTarget);
		er.setTargetCount(targetCount);
		er.setStudentCount(studentCount);
		er.setEduContent(eduContent);
		er.setEduMaterial(eduMaterial);
		er.setAbsenteeProcess(absenteeProcess);
		er.setEduEvaluation(eduEvaluation);
		er.set_audit(user);
		
		er = this.eduResultRepository.save(er);
		
		if (fileId != null && !fileId.isEmpty())  {
			
			Integer data_pk = er.getId();
			String[] fileIdList = fileId.split(",");
			
			for (String file_id : fileIdList) {
				int fid = Integer.parseInt(file_id);
				this.fileService.updateDataPk(fid, data_pk);
			}
		}

		result.data = er;
		
		return result;
	}
	
	@PostMapping("/delete")
	public AjaxResult deleteEduResult(
			@RequestParam(value = "id", required = false) Integer id) {
		
		AjaxResult result = new AjaxResult();
		
		this.eduResultRepository.deleteById(id);
		
		return result;
	}
	
	@GetMapping("/appr_stat")
	public AjaxResult apprStat(
			@RequestParam(value="start_date", required=false) String startDate, 
			@RequestParam(value="end_date", required=false) String endDate, 
			@RequestParam(value="appr_state", required=false) String apprState, 
			@RequestParam(value="edu_type", required=false) String eduType, 
			HttpServletRequest request) {
		
		Map<String,Object> items = this.eduResultService.apprStat(startDate,endDate,apprState, eduType);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}
	
	@GetMapping("/read_in")
	public AjaxResult getEduResult(
			@RequestParam(value="bh_id", required=false) Integer bhId,
			@RequestParam(value="data_date", required=false) String dataDate, 
			@RequestParam(value="edu_type", required=false) String eduType, 
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		Map<String, Object> items = this.eduResultService.getEduResult(bhId, dataDate,eduType, user);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
		
	}
	
	@PostMapping("/save_edu")
	@Transactional
	public AjaxResult saveEduResult(
			@RequestParam(value="bh_id", required=false) Integer bhId,
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam(value="title", required=false) String title, 
			@RequestParam(value="data_date", required=false) String data_date,
			@RequestParam(value="edu_type", required=false) String eduType, 
			@RequestParam(value="edu_date", required=false) String eduDate,
			@RequestParam(value="edu_title", required=false) String eduTitle, 
			@RequestParam(value="edu_place", required=false) String eduPlace,
			@RequestParam(value="teacher", required=false) String teacher, 
			@RequestParam(value="edu_target", required=false) String eduTarget,
			@RequestParam(value="student_count", required=false) Integer studentCount, 
			@RequestParam(value="target_count", required=false) Integer targetCount,
			@RequestParam(value="start_time", required=false) String startTime, 
			@RequestParam(value="end_time", required=false) String endTime,
			@RequestParam(value="edu_hour", required=false) Integer eduHour, 
			@RequestParam(value="edu_material", required=false) String eduMaterial,
			@RequestParam(value="absentee_process", required=false) String absenteeProcess, 
			@RequestParam(value="edu_content", required=false) String eduContent, 
			@RequestParam(value="edu_evaluation", required=false) String eduEvaluation,
			@RequestParam(value="fileId", required=false) String fileId,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) throws IOException {
		
		User user = (User)auth.getPrincipal();
		
		BundleHead bh = new BundleHead();
		
		Timestamp dataDate = Timestamp.valueOf(data_date+ " 00:00:00");
		
		List<Map<String, Object>> data = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		if (bhId > 0) {
			bh = this.bundleHeadRepository.getBundleHeadById(bhId);
		}
		
		bh.setTableName("edu_result_diary");
		bh.setChar1(title);
		bh.setDate1(dataDate);
		bh.setChar2(eduType);
		bh.set_audit(user);
		
		this.bundleHeadRepository.save(bh);
		
		EduResult er = new EduResult();
		if (id != null) {
			er = this.eduResultRepository.getEduResultById(id);
		} else {
			er.setSourceTableName("bundle_head");
		}
		
		er.setEduDate(Date.valueOf(eduDate));
		er.setEduTitle(eduTitle);
		er.setEduPlace(eduPlace);
		er.setTeacher(teacher);
		er.setEduHour(eduHour);
		er.setStartTime(startTime);
		er.setEndTime(endTime);
		er.setEduTarget(eduTarget);
		er.setTargetCount(targetCount);
		er.setStudentCount(studentCount);
		er.setEduContent(eduContent);
		er.setEduMaterial(eduMaterial);
		er.setAbsenteeProcess(absenteeProcess);
		er.setEduEvaluation(eduEvaluation);
		er.setSourceDataPk(bh.getId());
		er.set_audit(user);
		
		this.eduResultRepository.save(er);
		
		if (fileId != null && !fileId.isEmpty()) {
			Integer DataPk = er.getId();
			String[] fileIdList = fileId.split(",");
			
			for (int i = 0; i < fileIdList.length; i++) {
				fileService.updateDataPk(Integer.parseInt(fileIdList[i]), DataPk);
			}
		}
		
		
		List<EduResultStudent> ersList = this.eduResultStudentRepository.findByEduResultId(er.getId());
		
		// 참석자 명단 제거
		for (int i = 0; i < ersList.size(); i++) {
			this.eduResultStudentRepository.deleteById(ersList.get(i).getId());
		}
		
		// 참석자 명단 추가
		for (int i = 0; i < data.size(); i++) {
			EduResultStudent ers = new EduResultStudent();
			ers.setEduResultId(er.getId());
			ers.setStudentName(data.get(i).get("studentName").toString());
			ers.set_audit(user);
			this.eduResultStudentRepository.save(ers);
			
			this.uploadSign(ers.getId(),data.get(i).get("signImg").toString(),user);
		}
		
		
		AjaxResult result = new AjaxResult();
		
		result.success = true;
				
		Map<String, Object> items = new HashMap<>();
		items.put("id", bh.getId());
		
		result.data = items;
		
		return result;
	}
	
	@PostMapping("/delete_edu")
	@Transactional
	public AjaxResult deleteEdu(@RequestParam(value="bh_id", required=false) Integer bhId) {
		List<EduResult> er = this.eduResultRepository.findBySourceDataPkAndSourceTableName(bhId,"bundle_head");
			for (int i = 0; i < er.size(); i++ ) {
				this.eduResultRepository.deleteById(er.get(i).getId());
				
				List<EduResultStudent> ersList = this.eduResultStudentRepository.findByEduResultId(er.get(i).getId());
				
				// 참석자 명단 제거
				for (int j = 0; j < ersList.size(); j++) {
					this.eduResultStudentRepository.deleteById(ersList.get(j).getId());
				}
			}
		
		this.bundleHeadRepository.deleteById(bhId);
		
		AjaxResult result = new AjaxResult();
		
		result.success = true;
		
		return result;
	}
	
	// sign 업로드
	public void uploadSign(
			Integer dataPk, String param, User user) throws IOException {
		
		String sign = StringUtils.split(param, ",")[1];
		
		String saveFilePath = settings.getProperty("edu_sign");
		File saveDir = new File(saveFilePath);
		
		String fileName = dataPk + "_sign.png";
		
		// 디렉토리 없으면 생성
		if (!saveDir.isDirectory()) {
			saveDir.mkdir();
		}
		
		FileUtils.writeByteArrayToFile(new File(saveFilePath+fileName), Base64.decodeBase64(sign));
		
		// attachfile 삭제 및 저장 해야함
	}
	
	// sign 다운로드
	@GetMapping("/sign/download")
	public void signDown(
			@RequestParam(value="id", required = false) Integer id,
			HttpServletRequest request,
			HttpServletResponse response) {
		
		String filePath = settings.getProperty("edu_sign");
		String fileName = id + "_sign.png";
		
		try {
			// 경로와 파일명으로 파일 객체 생성
			File dFile = new File(filePath, fileName);
			
			// 파일 길이를 가져온다
			int fSize = (int) dFile.length();
			
			// 파일이 존재한 경우
			if (fSize > 0) {
				
				// ContentType 설정
				response.setContentType("image/png");
				
				// Header 설정
				response.setHeader("Content-Disposition", "attachment");
				
				// ContentLength 설정
				response.setContentLengthLong(fSize);
				
				BufferedInputStream in = null;
				BufferedOutputStream out = null;
				
				// 입력 스트림 생성
				in = new BufferedInputStream(new FileInputStream(dFile));
				
				// 츨력 스트림 생성
				out = new BufferedOutputStream(response.getOutputStream());
				
				try {
					
					byte[] buffer = new byte[4096];
					int bytesRead = 0;
					
					// 현재 파일 포인터 기준으로 함 
					while ((bytesRead = in.read(buffer)) != -1) {
						out.write(buffer, 0, bytesRead);
					}
					
					// 버퍼에 남은 내용이 있다면, 모두 파일에 출력
					out.flush();    					
				} catch (Exception e) {    					
					System.out.println(e.getMessage());
				} finally {						
					// 현재 열려있는 in, out 스트림 닫기
					if (in != null) {
						in.close();
					}
					
					if (out != null) {
						out.close();
						}						
					  }
					} else {
						throw new FileNotFoundException("파일이 없습니다.");
					}    			
				} catch (Exception ex) {
					System.out.println(ex.getMessage());
				}			
			}
	
}
