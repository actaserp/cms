package mes.app.files;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import io.micrometer.core.instrument.util.StringUtils;
import mes.app.common.service.FileService;
import mes.config.Settings;
import mes.domain.entity.AttachFile;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.AttachFileRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/files")
public class FilesController {

	@Autowired
	private FileService fileService;
	
	
	@Autowired
	FileService attachFileService;
	
	@Autowired
	AttachFileRepository attachFileRepository;

	@Autowired
	Settings settings;
	
	@PostMapping("/upload")
	public Object upload(
			MultipartHttpServletRequest multiRequest,
			@RequestParam("uploadfile") MultipartFile files,
			@RequestParam(value="DataPk", required = false) Integer DataPk,
			@RequestParam(value="tableName", required = false) String tableName,
			@RequestParam(value="attachName", required = false) String attachName,
			@RequestParam(value="onlyOne", required = false) Integer onlyOne,
			@RequestParam(value="others", required = false) String others,
			@RequestParam(value="accepts", required = false) String accepts,
			@RequestParam(value="addfileext", required = false) String addfileext,
			@RequestParam(value="thumbnailYN", required = false) String thumbnailYN,			
			RedirectAttributes redirectAttributes,
			Authentication auth) {
				
		if (DataPk == null || DataPk < 0) {
			DataPk = 0;
		}
		
		User user = (User)auth.getPrincipal();
		AttachFile attachFile = null;
		List<String> not_ext = Arrays.asList("py", "js", "aspx", "asp", "jsp", "php", "cs", "ini", "htaccess","exe","dll");
		AjaxResult result = new AjaxResult();
		
		Integer fileSize = (int) files.getSize();
		
		try {
			
			if (files != null) {
				
				if (fileSize > 52428800) { // 50m
					result.success = false;
					result.message = "Exceeded the allowed size";
					return result;
				}
			}
			
			String fileName = files.getOriginalFilename();
			
			String ext = fileName.substring(fileName.lastIndexOf(".") + 1).toLowerCase();
			
			if (StringUtils.isEmpty(accepts)==false) {
				if (accepts.contains(ext)==false) {		//accept에 ext가 들어있지 않은 경우
					result.success = false;
					result.message = "This file is not allowed to upload.";
					return result;
				}
			}
				
			if (not_ext.contains(ext)) { 		// 지원하지 않는 파일 list에 ext가 들어있는 경우
				result.success = false;
				result.message = "This file is not allowed to upload.";
				return result;
			}
			
			String path = settings.getProperty("file_upload_path") + others;
			
			// 2021-04-06 업무룰로 인한 추가
			if (attachName == null) {
				attachName = "basic";
			}

			try {
				// 트랜젝션 필요
				// 1. 파일저장
				String file_uuid_name = UUID.randomUUID().toString() + "." + ext;
				String saveFilePath = path ;
				File saveDir = new File(saveFilePath);
				MultipartFile mFile = null;
				
				mFile = files;
				
				// 디렉토리 없으면 생성
				if (!saveDir.isDirectory()) {
					saveDir.mkdir();
				}
				
				File saveFile = new File(path + File.separator + file_uuid_name);
				
				mFile.transferTo(saveFile);				
				
				if (onlyOne !=null && onlyOne != 0 && DataPk != 0) {
					// 구현안됨
					//attachFile = this.attachFileService.getAttachFileByData(tableName, DataPk, attachName);
					List<AttachFile> aList = this.attachFileRepository.findByTableNameAndDataPkAndAttachNameAndFileIndex(tableName,DataPk,attachName,0);
					
					if (aList.size() > 0) {
						attachFile = aList.get(0);						
					}
				}
				
				if (attachFile == null) {
					attachFile = new AttachFile();	
				}
				
				// attachFile 정보저장
				if (attachFile.getDataPk()==null) {	//attachFile이 비어있을 경우 
					attachFile.setTableName(tableName);
					attachFile.setDataPk(DataPk);
					attachFile.setAttachName(attachName);
				}
				
				attachFile.setPhysicFileName(file_uuid_name);
				attachFile.setFileIndex(0);
				attachFile.setFileName(fileName);
				attachFile.setExtName(ext);
				attachFile.setFilePath(path);
				attachFile.setFileSize(fileSize);
				attachFile.set_audit(user);
				
				
				attachFile = this.attachFileRepository.save(attachFile);
				
				result.data = attachFile;
				
				// 2. 썸네일파일 저장 ==>구현중				
				if("Y".equals(thumbnailYN)) {
					// 추후개발
					//thumb_path = settings.FILE_UPLOAD_PATH + others + "\\thumbnail\\"
					String thumb_path = settings.getProperty("FILE_UPLOAD_PATH") + others +"\\thumbnail\\"; 
					
					File thumbPath = new File(thumb_path);
					if(!thumbPath.isDirectory()) {
						thumbPath.mkdir();
					}
					
				}
				
			} catch(Exception e) {
				result.success = false;
				result.message = "업로드 오류";
			}
			
			
		} catch (Exception e) {
			result.success = false;
			result.message = "업로드 오류";
		}
		
		HashMap<String,Object> res = new HashMap<String,Object>();
		res.put("success", true);
		res.put("fileExt", attachFile.getExtName());
		res.put("fileNm", attachFile.getFileName());
		res.put("fileSize", attachFile.getFileSize());
		res.put("fileId", attachFile.getId());
		res.put("TableName", attachFile.getTableName());
		res.put("AttachName", attachFile.getAttachName());

		return res;
	}
				
	

	@GetMapping("/download")
	public void download(
			@RequestParam(value="file_id", required = false) Integer file_id,
			@RequestParam(value="TableName", required = false) String tableName,
			@RequestParam(value="AttachName", required = false) String attachName,
			HttpServletRequest request,
			HttpServletResponse response) throws Exception {
		
		Map<String, Object> row = fileService.getAttachFileDetail(file_id);
		
		if (row != null) {
			
			String FILE_UPLOAD_PATH = settings.getProperty("file_upload_path");
			
			String TableName = (String) row.get("TableName");
			//String AttachName = (String) row.get("AttachName");
			String PhysicFileName = (String) row.get("PhysicFileName");
			String FileName = (String) row.get("FileName");

			if (FILE_UPLOAD_PATH == null || FILE_UPLOAD_PATH == "") {
				throw new Exception("파일 경로가 설정되지 않았습니다.");
			}
			
			String filePath = FILE_UPLOAD_PATH + TableName + "\\";
    		String file_name = PhysicFileName;
    		
    		try {
    			// 경로와 파일명으로 파일 객체 생성
    			File dFile = new File(filePath, file_name);
    			
    			// 파일 길이를 가져온다
    			int fSize = (int) dFile.length();
    			
    			// 파일이 존재한 경우
    			if (fSize > 0) {
    				    				
    				// 파일명을 URLEncoder하여 attachment, Content-disposition Header로 설정
    				String encodedFilename = "attachment; filename*=UTF-8" + "''" + URLEncoder.encode(FileName, "UTF-8");
    				
    				// ContentType 설정
    				response.setContentType("application/excel; charset=utf-8");
    				
    				// Header 설정
    				response.setHeader("Content-Disposition", encodedFilename);
    				
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
	
	@GetMapping("/mes_form")
	public void mes_form(
			@RequestParam("file_name") String file_name,
			HttpServletRequest request,
			HttpServletResponse response) throws Exception {
    
		String file_path = settings.getProperty("mes_form_path");

		if (file_path == null || file_path == "") {
			throw new Exception("파일 경로가 설정되지 않았습니다.");
		}
		
		//String file_path = MES_FORM_PATH + tableName + "\\";
		
		try {
			// 경로와 파일명으로 파일 객체 생성
			File dFile = new File(file_path, file_name);
			
			// 파일 길이를 가져온다
			int fSize = (int) dFile.length();
			
			// 파일이 존재한 경우
			if (fSize > 0) {
				// 파일명을 URLEncoder하여 attachment, Content-disposition Header로 설정
				String encodedFilename = "attachment; filename*=UTF-8" + "''" + CommonUtil.getUtf8FileName(file_name);
				
				// ContentType 설정
				//response.setContentType("application/ms-excel");
				response.setContentType("application/excel; charset=utf-8");
				
				// Header 설정
				response.setHeader("Content-Disposition", encodedFilename);
				response.setHeader("Set-Cookie", "fileDownload=true; path=/");
				
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
