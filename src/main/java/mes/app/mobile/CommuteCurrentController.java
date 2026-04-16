package mes.app.mobile;

import mes.app.mobile.Service.CommuteCurrentService;
import mes.app.mobile.Service.MobileMainService;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/commute_current")
public class CommuteCurrentController {
    // 출퇴근현황
    @Autowired
    CommuteCurrentService commuteCurrentService;

    // 사용자 출퇴근 현황 조회
    @GetMapping("/read")
    public AjaxResult getUserInfo(
            @RequestParam(value="workcd", required = false) String workcd,
            @RequestParam(value="searchFromDate") String searchFromDate,
            @RequestParam(value="searchToDate") String searchToDate,
            HttpServletRequest request,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User)auth.getPrincipal();
        String username = user.getUsername();

        List<Map<String, Object>> data = commuteCurrentService.getUserInfo(username, workcd, searchFromDate, searchToDate);
        for(Map<String, Object>dataDetail : data) {
            String workym = (String) dataDetail.get("workym"); // YYYYMM
            String workday = (String) dataDetail.get("workday"); // DD

            if (workym != null && workday != null && workym.length() == 6 && workday.length() == 2) {
                String formattedDate = workym.substring(0, 4) + "." + workym.substring(4) + "." + workday;
                dataDetail.put("workym", formattedDate);
            }
        }
        result.data = data;

        return result;
    }
}
