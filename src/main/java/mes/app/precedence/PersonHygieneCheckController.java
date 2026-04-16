package mes.app.precedence;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.transaction.Transactional;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.common.service.DeviActionService;
import mes.app.precedence.service.PersonHygieneCheckService;
import mes.config.Settings;
import mes.domain.entity.BundleHead;
import mes.domain.entity.CheckMaster;
import mes.domain.entity.EduResultStudent;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BundleHeadRepository;
import mes.domain.repository.CheckItemResultRepository;
import mes.domain.repository.CheckMasterRepository;
import mes.domain.repository.CheckResultRepository;
import mes.domain.repository.EduResultStudentRepository;
import mes.domain.repository.PropDataRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/precedence/person_hygiene_check")
public class PersonHygieneCheckController {

	@Autowired
	private PersonHygieneCheckService personHygieneCheckService;
	
	@Autowired
	private DeviActionService deviActionService;
	
	@Autowired
	BundleHeadRepository bundleHeadRepository;
	
	@Autowired
	CheckMasterRepository checkMasterRepository;
	
	@Autowired
	CheckResultRepository checkResultRepository;
	
	@Autowired
	CheckItemResultRepository checkItemResultRepository;
	
	@Autowired
	PropDataRepository propDataRepository;
	
	@Autowired
	EduResultStudentRepository eduResultStudentRepository;
	
	@Autowired
	SqlRunner sqlRunner;
	
	@Autowired
	Settings settings;
	
	@GetMapping("/person_login_info")
	public AjaxResult getPersonLoginInfo(
			@RequestParam(value="login_id", required=false) String loginId) {
		
        List<Map<String, Object>> items = this.personHygieneCheckService.getPersonLoginInfo(loginId);      
        AjaxResult result = new AjaxResult();
        result.data = items;
		return result;
	}
	
	@GetMapping("/check_item_list")
	public AjaxResult getCheckItemList(
			@RequestParam(value="person_id", required=false) String personId,
			@RequestParam(value="data_date", required=false) String dataDate) {
		
		CheckMaster cm = this.checkMasterRepository.getByCode("개인위생체크리스트");
		
		Integer masterId = 0;
		
		if(cm.getId() != null) {
			masterId = cm.getId();
		}
		
        List<Map<String, Object>> items = this.personHygieneCheckService.getCheckItemList(personId,dataDate,masterId);
        
        List<Map<String, Object>> res = null;
        if(items.size() > 0) {
        	res = this.personHygieneCheckService.getCheckItemListFirst(items.get(0).get("id").toString());
        } else {
        	res = this.personHygieneCheckService.getCheckItemListSecond(masterId,dataDate);
        }
        
         AjaxResult result = new AjaxResult();
        result.data = res;
		return result;
	}
	
	// 결재현황 조회
	@GetMapping("/appr_stat")
	public AjaxResult getPersonCheckItemApprStatus(
			@RequestParam(value="start_date", required=false) String startDate, 
			@RequestParam(value="end_date", required=false) String endDate, 
			@RequestParam(value="appr_state", required=false) String apprState,
			@RequestParam(value="diary_type", required=false) String diaryType,
			HttpServletRequest request) {
		
        List<Map<String, Object>> items = this.personHygieneCheckService.getPersonCheckItemApprStatus(startDate,endDate,apprState,diaryType);     
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	// 점검표 조회
	@GetMapping("/read")
	public AjaxResult getPersonCheckItemList(
    		@RequestParam(value="bh_id", required=false) Integer bh_id, 
    		@RequestParam(value="diary_type", required=false) String diary_type,
			HttpServletRequest request) {
		
        Map<String, Object> items = this.personHygieneCheckService.getPersonCheckItemList(bh_id,diary_type);      
        AjaxResult result = new AjaxResult();
        result.data = items;
		return result;
	}
	
	// 부서별 사용자 조회
	@GetMapping("/read_user")
	public AjaxResult getPersonReadUser(
    		@RequestParam(value="dept_id", required=false) Integer deptId, 
			HttpServletRequest request) {
		
		List<Map<String, Object>> items = this.personHygieneCheckService.getPersonReadUser(deptId);      
        AjaxResult result = new AjaxResult();
        result.data = items;
		return result;
	}
	
	@PostMapping("/save")
	@Transactional
	public AjaxResult savePersonCheckItem(
	        @RequestParam(value = "bh_id", required = false) Integer bh_id,
	        @RequestParam(value = "check_master_id", required = false) Integer check_master_id,
	        @RequestParam(value = "title", required = false) String title,
	        @RequestParam(value = "data_date", required = false) String data_date,
	        @RequestParam(value = "diary_type", required = false) String diary_type,
	        @RequestBody MultiValueMap<String, Object> Q,
	        HttpServletRequest request,
	        Authentication auth) throws IOException {

	    AjaxResult result = new AjaxResult();
	    User user = (User) auth.getPrincipal();

	    BundleHead bh = new BundleHead();

	    if (bh_id > 0) {
	        bh = this.bundleHeadRepository.getBundleHeadById(bh_id);
	    } else {
	        bh.setTableName("person_hygiene_check");
	        bh.setChar1(title);
	        bh.setDate1(CommonUtil.tryTimestamp(data_date));
	        bh.setNumber1((float) check_master_id);
	        bh.setText1(diary_type);
	        bh.set_audit(user);
	        bh = this.bundleHeadRepository.save(bh);
	    }
	    Integer bhId = bh.getId();

	    List<Map<String, Object>> rowQItems = CommonUtil.loadJsonListMap(Q.getFirst("rowQ").toString());
	    List<List<Map<String, Object>>> colQItems = CommonUtil.loadJsonMulipleListMap(Q.getFirst("colQ").toString());
	    
	    if (rowQItems.isEmpty() || colQItems.isEmpty()) {
	        result.success = false;
	        return result;
	    }

	    for (int i = 0; i < rowQItems.size(); i++) {
	    	// check_result에 그리드 행 정보 저장
	        Integer check_result_id = this.personHygieneCheckService.saveCheckResultAndReturnId(i, rowQItems.get(i), bhId, check_master_id, data_date, diary_type, user);
	        
	        // this.personHygieneCheckService.uploadSign(check_result_id, CommonUtil.tryString(rowQItems.get(i).get("img_src")), user);
	        // check_item_result에 item0 ~ item5 o,x 값을 피벗하여 저장
	        for (int j = 0; j < colQItems.get(i).size(); j++) {
	        	this.personHygieneCheckService.saveCheckItemResult(colQItems.get(i).get(j), check_result_id, user);
	        }
	    }
	    
	    MapSqlParameterSource dicParam = new MapSqlParameterSource();     
        dicParam.addValue("bh_id", bhId);
        
	    if(bh_id > 0) {
	        String sql = """
	        	select cir.id as src_data_pk ,'person_hygiene_check' as source_table_name , cr."CheckDate" as happen_date, cm."Name" as happen_place
	            , concat(cr."CheckerName",' : ', coalesce(ci."ItemGroup1",' '), coalesce(ci."ItemGroup2", ' '), coalesce(ci."ItemGroup3",' '), ci."Name") as abnormal_detail
	            ,null as action_detail ,null as confirm_detail
	            from check_result cr
	            inner join check_item_result cir on cir."CheckResult_id" = cr.id
	            inner join check_item ci on ci.id = cir."CheckItem_id"
	            inner join check_mast cm on cm.id = cr."CheckMaster_id"	
	            where cir.id not in (
		            select a."SourceDataPk"
		            from devi_action a
	                inner join check_item_result b on a."SourceDataPk" = b.id
	                inner join check_result c on b."CheckResult_id" = c.id and c."SourceDataPk" = :bh_id
		            where a."SourceTableName" = 'person_hygiene_check'
	            )
	            and cr."SourceDataPk" = :bh_id
                and cir."Result1" = 'X' and ci."Name" <>'판정'
        		""";
	  			    
	        List<Map<String, Object>> devi = this.sqlRunner.getRows(sql, dicParam);
	        if (!devi.isEmpty()) {
	            for (Map<String, Object> item : devi) {
	                Integer srcDataPk = CommonUtil.tryIntNull(item.get("src_data_pk"));
	                String sourceTableName = CommonUtil.tryString(item.get("source_table_name"));
	                String happenDate = CommonUtil.tryString(item.get("happen_date"));
	                String happenPlace = CommonUtil.tryString(item.get("happen_place"));
	                String abnormalDetail = CommonUtil.tryString(item.get("abnormal_detail"));
	                String actionDetail = CommonUtil.tryString(item.get("action_detail"));
	                String confirmDetail = CommonUtil.tryString(item.get("confirm_detail"));
	                this.deviActionService.saveDeviAction(0, srcDataPk, sourceTableName, happenDate, happenPlace, abnormalDetail, actionDetail, confirmDetail, user);
	            }
	        }
	        
	        sql = """
	        	select id from devi_action da
                where da."SourceTableName" = 'person_hygiene_check'
                and da."SourceDataPk" not in 
                (
                    select b.id
	                from check_result a
	                inner join check_item_result b on a.id = b."CheckResult_id" and b."Result1" = 'X'
	                inner join check_item ci on ci.id = b."CheckItem_id"
	                left join  devi_action c on c."SourceDataPk" = b.id
	                where b."Result1" = 'X' and ci."Name" <>'판정'
                )
        		""";
	        
	        List<Map<String, Object>> del_devi = this.sqlRunner.getRows(sql, dicParam);
	        if (!del_devi.isEmpty()) {
	            for (Map<String, Object> item : del_devi) {
	                Integer id = CommonUtil.tryIntNull(item.get("id"));
	                this.deviActionService.deleteDeviAction(id);
	            }
	        }
	    }else {
	    	String sql = """
	        	select cir.id as src_data_pk ,'person_hygiene_check' as source_table_name , cr."CheckDate" as happen_date, cm."Name" as happen_place
	            , concat(cr."CheckerName",' : ', coalesce(ci."ItemGroup1",' '), coalesce(ci."ItemGroup2", ' '), coalesce(ci."ItemGroup3",' '), ci."Name") as abnormal_detail
	            ,null as action_detail ,null as confirm_detail
	            from check_result cr
	            inner join check_item_result cir on cir."CheckResult_id" = cr.id
	            inner join check_item ci on ci.id = cir."CheckItem_id"
	            inner join check_mast cm on cm.id = cr."CheckMaster_id"	
	            where cr."SourceDataPk" = :bh_id
                and cir."Result1" = 'X' and ci."Name" <>'판정'
        		""";
		    
		    List<Map<String, Object>> devi = this.sqlRunner.getRows(sql, dicParam);
		    if (!devi.isEmpty()) {
	            for (Map<String, Object> item : devi) {
	                Integer srcDataPk = CommonUtil.tryIntNull(item.get("src_data_pk"));
	                String sourceTableName = CommonUtil.tryString(item.get("source_table_name"));
	                String happenDate = CommonUtil.tryString(item.get("happen_date"));
	                String happenPlace = CommonUtil.tryString(item.get("happen_place"));
	                String abnormalDetail = CommonUtil.tryString(item.get("abnormal_detail"));
	                String actionDetail = CommonUtil.tryString(item.get("action_detail"));
	                String confirmDetail = CommonUtil.tryString(item.get("confirm_detail"));
	                this.deviActionService.saveDeviAction(0, srcDataPk, sourceTableName, happenDate, happenPlace, abnormalDetail, actionDetail, confirmDetail, user);
	            }
	        }
	    }

		List<EduResultStudent> ersList = this.eduResultStudentRepository.findBySourceDataPkAndSourceTableName(bhId,"bundle_head");
		
		// 참석자 명단 제거
		for (int i = 0; i < ersList.size(); i++) {
			this.eduResultStudentRepository.deleteById(ersList.get(i).getId());
		}
	    
		List<Map<String, Object>> tableData = CommonUtil.loadJsonListMap(Q.getFirst("tableQ").toString());
		
		// 참석자 명단 추가
		for (int i = 0; i < tableData.size(); i++) {
			EduResultStudent ers = new EduResultStudent();
			ers.setSourceDataPk(bhId);
			ers.setSourceTableName("bundle_head");
			ers.setEduResultId(0);
			ers.setStudentName(tableData.get(i).get("studentName").toString());
			ers.set_audit(user);
			this.eduResultStudentRepository.save(ers);
			
			this.uploadSign(ers.getId(),tableData.get(i).get("signImg").toString(),user);
		}
		
	    Map<String, Object> item = new HashMap<String,Object>();
	    item.put("id", bhId);
	    result.success = true;
	    result.data = item;
	    return result;
	}
	
	@PostMapping("/delete")
	@Transactional
	public AjaxResult deleteVerifiCheckHaccpNormalList(
			@RequestParam(value="bh_id", required=false, defaultValue= "0") Integer bh_id) {
		
		AjaxResult result = new AjaxResult();
			        
		this.personHygieneCheckService.deletePersonCheckItemList(bh_id);
		
		List<EduResultStudent> ersList = this.eduResultStudentRepository.findBySourceDataPkAndSourceTableName(bh_id,"bundle_head");
		
		// 참석자 명단 제거
		for (int j = 0; j < ersList.size(); j++) {
			this.eduResultStudentRepository.deleteById(ersList.get(j).getId());
		}
		
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
