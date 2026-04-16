package mes.app.files;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URLEncoder;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import lombok.extern.slf4j.Slf4j;
import mes.app.common.TenantContext;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
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
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

@Slf4j
@RestController
@RequestMapping("/api/files")
public class FilesController {

	@Autowired
	SqlRunner sqlRunner;

	@Autowired
	NcpObjectStorageService storageService;

	private static final List<String> BLOCKED_EXT = Arrays.asList(
			"py", "js", "aspx", "asp", "jsp", "php", "cs", "ini", "htaccess", "exe", "dll");

	private static final DateTimeFormatter YYYYMMDD = DateTimeFormatter.ofPattern("yyyyMMdd");


	@PostMapping("/upload")
	public Object upload(
			MultipartHttpServletRequest multiRequest,
			@RequestParam("uploadfile") MultipartFile file,
			@RequestParam(value = "DataPk", required = false) Integer dataPk,
			@RequestParam(value = "tableName", required = false) String tableName,
			@RequestParam(value = "attachName", required = false) String attachName,
			@RequestParam(value = "accepts", required = false) String accepts,
			RedirectAttributes redirectAttributes,
			Authentication auth) {

		AjaxResult result = new AjaxResult();
		if (dataPk == null || dataPk < 0) dataPk = 0;
		if (attachName == null) attachName = "basic";

		User user = (User) auth.getPrincipal();
		String spjangcd = user.getSpjangcd();

		try {
			long fileSize = file.getSize();
			if (fileSize > 20971520L) { // 20MB
				result.success = false;
				result.message = "파일 크기가 20MB를 초과합니다.";
				return result;
			}

			String fileName = file.getOriginalFilename();
			String ext = fileName.substring(fileName.lastIndexOf(".") + 1).toLowerCase();

			if (!StringUtils.isEmpty(accepts) && !accepts.contains(ext)) {
				result.success = false;
				result.message = "허용되지 않는 파일 형식입니다.";
				return result;
			}
			if (BLOCKED_EXT.contains(ext)) {
				result.success = false;
				result.message = "허용되지 않는 파일 형식입니다.";
				return result;
			}

			// tableName = 기능 식별자 (예: NOTICE, QNA) — TB_FILEINFO.CHECKSEQ 와 동일
			String uuidFileName = UUID.randomUUID().toString() + "." + ext;
			String objectKey = storageService.buildObjectKey(spjangcd, tableName, uuidFileName);
			String filePrefix = storageService.getFilePrefix(spjangcd, tableName);

			java.io.File tempFile = java.io.File.createTempFile("upload_", "." + ext);
			try {
				file.transferTo(tempFile);
				try (java.io.FileInputStream fis = new java.io.FileInputStream(tempFile)) {
					storageService.upload(objectKey, fis, fileSize,
							file.getContentType() != null ? file.getContentType() : "application/octet-stream");
				}
			} finally {
				tempFile.delete(); // 성공/실패 상관없이 항상 삭제
			}

			// TB_FILEINFO에 메타데이터 저장 (MSSQL, OUTPUT으로 PK 수신)
			String today = LocalDate.now().format(YYYYMMDD);
			MapSqlParameterSource paramMap = new MapSqlParameterSource();
			String username = user.getUsername();
			if (username != null && username.length() > 20) username = username.substring(0, 20);

			paramMap.addValue("filedate", today);
			paramMap.addValue("checkseq", NcpObjectStorageService.toCheckseq(tableName));   // varchar(2)
			paramMap.addValue("bbsseq", dataPk);
			paramMap.addValue("filepath", filePrefix);
			paramMap.addValue("filesvnm", uuidFileName);
			paramMap.addValue("fileextns", ext);
			paramMap.addValue("fileornm", fileName);
			paramMap.addValue("filesize", (int) fileSize);
			paramMap.addValue("inuserid", username);
			paramMap.addValue("spjangcd", spjangcd);

			// FILEURL 은 FILEPATH + '/' + FILESVNM 으로 조합 가능하므로 저장하지 않음
			String sql = """
					INSERT INTO TB_FILEINFO
						 (FILEDATE, CHECKSEQ, bbsseq, FILEPATH, FILESVNM, FILEEXTNS, FILEORNM, FILESIZE, INDATEM, INUSERID, spjangcd)
					 VALUES
						 (:filedate, :checkseq, :bbsseq, :filepath, :filesvnm, :fileextns, :fileornm, :filesize, NOW(), :inuserid, :spjangcd)
					 RETURNING fileseq
					""";

			Map<String, Object> row = sqlRunner.getRow(sql, paramMap);
			if (row == null) {
				result.success = false;
				result.message = "파일 정보 저장 실패";
				return result;
			}

			int fileseq = ((Number) row.get("fileseq")).intValue();

			HashMap<String, Object> res = new HashMap<>();
			res.put("success", true);
			res.put("fileExt", ext);
			res.put("fileNm", fileName);
			res.put("fileSize", (int) fileSize);
			res.put("fileId", fileseq);
			res.put("TableName", tableName);
			res.put("AttachName", attachName);
			return res;

		} catch (Exception e) {
			log.error("[FileUpload] 업로드 오류: {}", e.getMessage(), e);
			result.success = false;
			result.message = "업로드 오류: " + e.getMessage();
			return result;
		}
	}

	@GetMapping("/master-guide")
	public void masterGuide(HttpServletResponse response, Authentication auth) throws Exception {

		User user = (User) auth.getPrincipal();
		String groupCode = user.getUserProfile().getUserGroup().getCode();

		if (!"Master".equals(groupCode)) {
			response.sendError(HttpServletResponse.SC_FORBIDDEN, "접근 권한이 없습니다.");
			return;
		}

		String objectKey = "guide/ACTAS_시스템설정_매뉴얼.pdf";

		try (ResponseInputStream<GetObjectResponse> s3Stream = storageService.download(objectKey);
			 BufferedOutputStream out = new BufferedOutputStream(response.getOutputStream())) {

			response.setContentType("application/pdf");
			response.setHeader("Content-Disposition",
					"inline; filename*=UTF-8''ACTAS_%EC%8B%9C%EC%8A%A4%ED%85%9C%EC%84%A4%EC%A0%95_%EB%A7%A4%EB%89%B4%EC%96%BC.pdf");

			byte[] buffer = new byte[8192];
			int bytesRead;
			while ((bytesRead = s3Stream.read(buffer)) != -1) {
				out.write(buffer, 0, bytesRead);
			}
			out.flush();

		} catch (Exception e) {
			log.error("[MasterGuide] 다운로드 오류: {}", e.getMessage(), e);
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "다운로드 오류");
		}
	}

	@GetMapping("/download")
	public void download(
			@RequestParam("file_id") Integer fileseq,
			HttpServletRequest request,
			HttpServletResponse response) throws Exception {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("fileseq", fileseq);

		Map<String, Object> row = sqlRunner.getRow(
				"select FILEPATH, FILESVNM, FILEORNM from TB_FILEINFO where fileseq = :fileseq", paramMap);

		if (row == null) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, "파일을 찾을 수 없습니다.");
			return;
		}

		String objectKey = row.get("FILEPATH") + "/" + row.get("FILESVNM");
		String fileName  = (String) row.get("FILEORNM");

		try (ResponseInputStream<GetObjectResponse> s3Stream = storageService.download(objectKey);
			 BufferedOutputStream out = new BufferedOutputStream(response.getOutputStream())) {

			String encodedFilename = "attachment; filename*=UTF-8''" + URLEncoder.encode(fileName, "UTF-8");
			response.setContentType("application/octet-stream");
			response.setHeader("Content-Disposition", encodedFilename);

			byte[] buffer = new byte[8192];
			int bytesRead;
			while ((bytesRead = s3Stream.read(buffer)) != -1) {
				out.write(buffer, 0, bytesRead);
			}
			out.flush();

		} catch (Exception e) {
			log.error("[FileDownload] 다운로드 오류 (key={}): {}", objectKey, e.getMessage(), e);
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "다운로드 오류");
		}
	}
}
