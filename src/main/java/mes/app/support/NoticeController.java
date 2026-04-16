package mes.app.support;

import java.sql.Date;
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

import mes.app.common.service.FileService;
import mes.app.support.service.NoticeService;
import mes.domain.entity.Board;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BoardRepository;
import mes.domain.services.DateUtil;

@RestController
@RequestMapping("/api/support/notice")
public class NoticeController {
	
	@Autowired
	private NoticeService noticeService;
	
	@Autowired
	BoardRepository boardRepository;

	@Autowired
	private FileService fileService;
	
	// 공지사항 조회
	@GetMapping("/read")
	public AjaxResult getBoardList(
			@RequestParam(value = "srchStartDt", required = false) String srchStartDt,
			@RequestParam(value = "srchEndDt", required = false) String srchEndDt,
			@RequestParam(value = "keyword", required = false) String keyword,
			HttpServletRequest request) {

        String date_from = srchStartDt + " 00:00:00";
        String date_to = srchEndDt + " 23:59:59";
        
		List<Map<String, Object>> items = this.noticeService.getBoardList("notice", keyword, date_from, date_to);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}
		
	// 공지사항 상세 조회
	@GetMapping("/detail")
	public AjaxResult getBoardDetail(
			@RequestParam("id") int id, 
			HttpServletRequest request) {
		
        Map<String, Object> items = this.noticeService.getBoardDetail(id);
        
        AjaxResult result = new AjaxResult();
        result.data = items;
        
		return result;
	}
		
	// 공지사항 저장
	@PostMapping("/save")
	public AjaxResult saveBoard(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam(value="title", required=false) String title,
			@RequestParam(value="content", required=false) String content,
			@RequestParam(value="notice_yn", required=false) String notice_yn,
			@RequestParam(value="notice_end_date", required=false) Date notice_end_date,
			@RequestParam(value="fileId", required=false) String file_id,
			HttpServletRequest request,
			Authentication auth) {

        AjaxResult result = new AjaxResult();
		User user = (User)auth.getPrincipal();
		Board board = null;
		
		if (id == null) {
			board = new Board();
		} else {
			board = this.boardRepository.getBoardById(id);
		}

        // fileId = posparam.get('fileId')
      	board.setBoardGroup("notice");
		board.setTitle(title);
		board.setContent(content);
        board.setNoticeYN (notice_yn);
        board.setNoticeEndDate(notice_end_date);
        board.setWriteDateTime(DateUtil.getNowTimeStamp());
		board.set_audit(user);
		
		board = this.boardRepository.save(board);
	
		if (file_id != null && file_id != "") {
			Integer DataPk = board.getId();

			String[] fileIdList = file_id.split(",");
			
			for (String fileId : fileIdList) {
				int fileid = Integer.parseInt(fileId);
				this.fileService.updateDataPk(fileid, DataPk);
			}
		}
		
        result.data = board;
		
		return result;
	}

	// 공지사항 삭제
	@PostMapping("/delete")
	public AjaxResult deleteBoard(@RequestParam("id") Integer id) {
		this.boardRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		return result;
	}

}
