package mes.app.common;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import lombok.extern.slf4j.Slf4j;
import mes.app.files.NcpObjectStorageService;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.common.service.FileService;
import mes.domain.entity.AttachFile;
import mes.domain.model.AjaxResult;
import mes.domain.repository.AttachFileRepository;
import mes.domain.repository.FileRepository;

@Slf4j
@RestController
@RequestMapping("/api/common/attach_file")
public class AttachFileController {

	@Autowired
	SqlRunner sqlRunner;

	@Autowired
	NcpObjectStorageService storageService;

	// 첨부파일 목록 조회 (ax5_uploader용)
	@GetMapping("/detailFiles")
	public AjaxResult detailFiles(
			@RequestParam("TableName") String tableName,
			@RequestParam("DataPk") Integer dataPk,
			@RequestParam(value = "attachName", required = false) String attachName,
			HttpServletRequest request) {

		String spjangcd = TenantContext.get();
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("checkseq", NcpObjectStorageService.toCheckseq(tableName));
		paramMap.addValue("bbsseq", dataPk);
		paramMap.addValue("spjangcd", spjangcd);

		String sql = """
				select fileseq as "fileId"
				, FILEORNM as "fileNm"
				, FILEEXTNS as "fileExt"
				, FILESIZE as "fileSize"
				, FILEURL
				from TB_FILEINFO
				where CHECKSEQ = :checkseq
				and bbsseq = :bbsseq
				and spjangcd = :spjangcd
				order by fileseq
				""";

		List<Map<String, Object>> items = sqlRunner.getRows(sql, paramMap);

		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}

	// 첨부파일 삭제 (NCP + TB_FILEINFO)
	@PostMapping("/deleteFile")
	public AjaxResult deleteFile(
			@RequestParam(value = "fileId") Integer fileseq,
			HttpServletRequest request) {

		AjaxResult result = new AjaxResult();

		// NCP 오브젝트 키 조회
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("fileseq", fileseq);

		Map<String, Object> row = sqlRunner.getRow(
				"select FILEPATH, FILESVNM from TB_FILEINFO where fileseq = :fileseq", paramMap);

		if (row == null) {
			result.success = false;
			result.message = "파일을 찾을 수 없습니다.";
			return result;
		}

		// NCP에서 삭제
		try {
			String objectKey = row.get("FILEPATH") + "/" + row.get("FILESVNM");
			storageService.delete(objectKey);
		} catch (Exception e) {
			log.error("[FileDelete] NCP 삭제 오류 (fileseq={}): {}", fileseq, e.getMessage(), e);
		}

		// TB_FILEINFO에서 삭제
		sqlRunner.execute("delete from TB_FILEINFO where fileseq = :fileseq", paramMap);

		return result;
	}
}
