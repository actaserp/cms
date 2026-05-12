package mes.app.cms;

import mes.app.cms.service.CmsHolidayService;
import mes.domain.model.AjaxResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/cms/holiday")
public class CmsHolidayController {

    @Autowired
    private CmsHolidayService cmsHolidayService;

    /**
     * 월별 휴일 목록 조회 (프론트 캘린더용)
     * GET /api/cms/holiday/list?ym=202505
     */
    @GetMapping("/list")
    public AjaxResult getHolidayList(@RequestParam("ym") String ym) {
        AjaxResult result = new AjaxResult();
        try {
            List<String> holidays = cmsHolidayService.getHolidayList(ym);
            result.data = holidays;
        } catch (Exception e) {
            result.success = false;
            result.message = e.getMessage();
        }
        return result;
    }

    /**
     * 특정 날짜 기준 다음 영업일 조회
     * GET /api/cms/holiday/next-business-day?date=20250530
     */
    @GetMapping("/next-business-day")
    public AjaxResult getNextBusinessDay(@RequestParam("date") String date) {
        AjaxResult result = new AjaxResult();
        try {
            result.data = cmsHolidayService.getNextBusinessDay(date);
        } catch (Exception e) {
            result.success = false;
            result.message = e.getMessage();
        }
        return result;
    }
}