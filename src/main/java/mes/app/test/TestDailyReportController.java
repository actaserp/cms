package mes.app.test;

import lombok.extern.slf4j.Slf4j;
import mes.app.test.service.TestDailyReportService;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.inspection_reportsRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import javax.transaction.Transactional;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/test/test_daily_report")
public class TestDailyReportController { // 검사일보

  @Autowired
  TestDailyReportService testDailyReportService;

  @Autowired
  inspection_reportsRepository inspectionReportsRepository;

  @GetMapping("/read")
  public AjaxResult getTestDailyReportRead(@RequestParam(value = "WorkCenter_id") Integer workCenterId,
                                           @RequestParam(value = "SearchDate") String searchDate) {
    LocalDate startDate = LocalDate.parse(searchDate + "-01");
    LocalDate endDate = startDate.withDayOfMonth(startDate.lengthOfMonth()); // 월의 마지막 일

    String start = startDate.toString();
    String end = endDate.toString();

    List<Map<String, Object>> items = testDailyReportService.getList(workCenterId,start,end );
    AjaxResult result = new AjaxResult();
    result.data = items;

    return result;
  }

  @GetMapping("/detail")
  public AjaxResult getTestDailtdetail(@RequestParam(value = "WorkCenter_id")Integer work_center_id,
                                       @RequestParam(value = "SearchDate") String search_date,
                                       @RequestParam(value = "defect_pk") Integer defect_pk) {

//    log.info("검사일지 detail -- work_center_id:{}, searchDate:{},defect_pk:{} ", work_center_id, search_date, defect_pk);
    Map<String, Object> item = testDailyReportService.getDetail(work_center_id,search_date,defect_pk);

    AjaxResult result = new AjaxResult();
    result.data = item;
    return result;
  }

  @PostMapping("/save")
  @Transactional
  public AjaxResult save(@RequestBody Map<String, Object> payload, Authentication auth) {
    var header = (Map<String, Object>) payload.get("header");
    var lines  = (List<Map<String, Object>>) payload.get("lines");

//    log.info("header:{}, lines:{}", header, lines);

    Integer workCenterId = toInt(header.get("workCenterId"));
    LocalDate inspectionDate = LocalDate.parse((String) header.get("inspectionDate"));
    Double inspectionQty = header.get("inspectionQty") == null ? null :
    Double.valueOf(header.get("inspectionQty").toString());
    String spjangcd  = (String) header.get("spjangcd");

    var user = (User) auth.getPrincipal();

    testDailyReportService.saveReplacing(
        workCenterId, inspectionDate, inspectionQty, spjangcd, user.getId(), lines
    );

    AjaxResult result = new AjaxResult();
    result.success = true;
    return result;
  }

  @GetMapping("/defects")
  public AjaxResult defectsList (@RequestParam(value ="workCenterId") Integer work_id,
                                 @RequestParam(value = "date") String date) {
    AjaxResult result = new AjaxResult();

//    log.info("work_id:{}, date:{}", work_id, date);
    try {
      List<Map<String, Object>> items = testDailyReportService.defectsWithQty(work_id, date);
      result.success = true;
      result.data = items;
      return result;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  private Integer toInt(Object o) {
    if (o == null) return null;
    if (o instanceof Number n) return n.intValue();
    String s = o.toString().trim().replace(",", "");
    return s.isEmpty() ? null : Integer.valueOf(s);
  }

}
