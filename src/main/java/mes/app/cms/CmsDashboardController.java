package mes.app.cms;

import mes.app.cms.service.CmsDashboardService;
import mes.domain.model.AjaxResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/api/cms/dashboard")
public class CmsDashboardController {

    @Autowired
    private CmsDashboardService cmsDashboardService;

    @GetMapping("/summary")
    public AjaxResult getSummary(@RequestParam("ym") String ym, HttpServletRequest request) {
        AjaxResult result = new AjaxResult();
        result.data = cmsDashboardService.getSummary(ym);
        return result;
    }

    @GetMapping("/calendar")
    public AjaxResult getCalendar(@RequestParam("ym") String ym, HttpServletRequest request) {
        AjaxResult result = new AjaxResult();
        result.data = cmsDashboardService.getCalendarData(ym);
        return result;
    }

    @GetMapping("/daily")
    public AjaxResult getDaily(@RequestParam("date") String date, HttpServletRequest request) {
        AjaxResult result = new AjaxResult();
        result.data = cmsDashboardService.getDailyList(date);
        return result;
    }
}
