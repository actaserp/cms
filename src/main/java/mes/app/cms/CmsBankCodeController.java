package mes.app.cms;

import mes.app.cms.service.CmsBankCodeService;
import mes.domain.model.AjaxResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/cms/bank_code")
public class CmsBankCodeController {

    @Autowired
    private CmsBankCodeService cmsBankCodeService;

    /** 목록 조회 */
    @GetMapping("/list")
    public AjaxResult getList(
            @RequestParam(value = "bank_name", required = false) String bankName) {
        List<Map<String, Object>> items = cmsBankCodeService.getBankCodeList(bankName);
        AjaxResult result = new AjaxResult();
        result.data = items;
        return result;
    }

    /** 저장 (신규/수정) */
    @PostMapping("/save")
    public AjaxResult save(
            @RequestParam(value = "id",        required = false) Long   id,
            @RequestParam("bank_code")                           String bankCode,
            @RequestParam("bank_name")                           String bankName,
            @RequestParam(value = "use_yn",    required = false) String useYn) {

        AjaxResult result = new AjaxResult();
        Long savedId = cmsBankCodeService.saveBankCode(id, bankCode, bankName, useYn);
        if (savedId == null) {
            result.success = false;
            result.message = "저장에 실패했습니다.";
        } else {
            result.data = savedId;
        }
        return result;
    }

    /** 삭제 */
    @PostMapping("/delete")
    public AjaxResult delete(@RequestParam("id") Long id) {
        AjaxResult result = new AjaxResult();
        if (!cmsBankCodeService.deleteBankCode(id)) {
            result.success = false;
            result.message = "삭제에 실패했습니다.";
        }
        return result;
    }
}
