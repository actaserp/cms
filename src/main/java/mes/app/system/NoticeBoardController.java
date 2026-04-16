package mes.app.system;

import mes.app.system.service.NoticeService;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/support/notice")
public class NoticeBoardController {

	@Autowired
	private NoticeService noticeService;

	// 공지사항 목록 조회
	@GetMapping("/read")
	public AjaxResult getBoardList(
			@RequestParam(value = "srchStartDt", required = false) String srchStartDt,
			@RequestParam(value = "srchEndDt", required = false) String srchEndDt,
			@RequestParam(value = "keyword", required = false) String keyword,
			HttpServletRequest request) {

		// 프론트에서 yyyy-MM-dd 로 오므로 yyyyMMdd 로 변환
		String dateFrom = (srchStartDt != null) ? srchStartDt.replace("-", "") : "";
		String dateTo   = (srchEndDt   != null) ? srchEndDt.replace("-", "")   : "";

		List<Map<String, Object>> items = this.noticeService.getBoardList(keyword, dateFrom, dateTo);

		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}

	// 공지사항 상세 조회
	@GetMapping("/detail")
	public AjaxResult getBoardDetail(
			@RequestParam("id") int id,
			HttpServletRequest request) {

		Map<String, Object> item = this.noticeService.getBoardDetail(id);

		AjaxResult result = new AjaxResult();
		result.data = item;
		return result;
	}

	// 활성 공지 조회 (index 팝업용)
	@GetMapping("/active")
	public AjaxResult getActiveNotices() {
		AjaxResult result = new AjaxResult();
		result.data = this.noticeService.getActiveNotices();
		return result;
	}

	// 공지사항 저장 (신규/수정)
	@PostMapping("/save")
	public AjaxResult saveBoard(
			@RequestParam(value = "id", required = false) Integer id,
			@RequestParam(value = "title", required = false) String title,
			@RequestParam(value = "content", required = false) String content,
			@RequestParam(value = "notice_yn", required = false) String notice_yn,
			@RequestParam(value = "notice_from_date", required = false) String notice_from_date,
			@RequestParam(value = "notice_end_date", required = false) String notice_end_date,
			HttpServletRequest request,
			Authentication auth) {

		User user = (User) auth.getPrincipal();

		Integer savedId = this.noticeService.saveNotice(
				id, title, content, notice_yn, notice_from_date, notice_end_date, user.getUsername());

		AjaxResult result = new AjaxResult();
		if (savedId == null) {
			result.success = false;
			result.message = "저장에 실패했습니다. 로그를 확인하세요.";
		} else {
			result.data = savedId;
		}
		return result;
	}

	// 공지사항 삭제
	@PostMapping("/delete")
	public AjaxResult deleteBoard(@RequestParam("id") Integer id) {
		this.noticeService.deleteNotice(id);
		return new AjaxResult();
	}
}
