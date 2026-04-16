package mes.app.system;

import lombok.extern.slf4j.Slf4j;
import mes.app.mobile.Service.MobileMainService;
import mes.app.system.service.CustomerCurrentService;
import mes.domain.model.AjaxResult;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/system/customer_current")
public class CustomerCurrentController {

    @Autowired
    CustomerCurrentService customerCurrentService;

    @GetMapping("/read")
    public AjaxResult getList(@RequestParam String srchStartDt,
                              @RequestParam String srchEndDt,
                              @RequestParam(required = false) String keyword
    ){
        AjaxResult result = new AjaxResult();

        List<Map<String, Object>> items = this.customerCurrentService.getCustomerList(srchStartDt, srchEndDt, keyword);

        result.data = items;

        return result;
    }

    @PostMapping("/batchApprove")
    public AjaxResult batchApprove(@RequestBody Map<String, Object> param) {
        AjaxResult result = new AjaxResult();

        List<String> ids = (List<String>) param.get("ids");

        if (ids == null || ids.isEmpty()) {
            result.success = false;
            result.message = "승인할 사업체를 선택해주세요.";
            return result;
        }

        this.customerCurrentService.batchApprove(ids);
        result.success = true;
        result.message = "승인 처리가 완료되었습니다.";

        return result;
    }

    @PostMapping("/btnStop")
    public AjaxResult btnStop(@RequestBody Map<String, Object> param) {
        AjaxResult result = new AjaxResult();

        List<String> ids = (List<String>) param.get("ids");

        if (ids == null || ids.isEmpty()) {
            result.success = false;
            result.message = "중지할 사업체를 선택해주세요.";
            return result;
        }

        this.customerCurrentService.btnStop(ids);
        result.success = true;
        result.message = "중지 처리가 완료되었습니다.";

        return result;
    }
}
