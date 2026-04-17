package mes.app.cms;

import mes.app.cms.service.CmsMemberService;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/cms/member")
public class CmsMemberController {

    @Autowired
    private CmsMemberService cmsMemberService;

    /** 목록 조회 */
    @GetMapping("/list")
    public AjaxResult getList(
            @RequestParam(value = "member_name", required = false) String memberName,
            @RequestParam(value = "member_no",   required = false) String memberNo,
            @RequestParam(value = "status",      required = false) String status,
            HttpServletRequest request) {

        List<Map<String, Object>> items = cmsMemberService.getMemberList(memberName, memberNo, status);
        AjaxResult result = new AjaxResult();
        result.data = items;
        return result;
    }

    /** 단건 조회 */
    @GetMapping("/{id}")
    public AjaxResult getOne(@PathVariable Long id) {
        Map<String, Object> item = cmsMemberService.getMember(id);
        AjaxResult result = new AjaxResult();
        result.data = item;
        return result;
    }

    /** 등록/수정 */
    @PostMapping("/save")
    public AjaxResult save(
            @RequestParam(value = "id",             required = false) Long    id,
            @RequestParam(value = "member_type",    required = false) String  memberType,
            @RequestParam(value = "member_name"                     ) String  memberName,
            @RequestParam(value = "member_no",      required = false) String  memberNo,
            @RequestParam(value = "id_number",      required = false) String  idNumber,
            @RequestParam(value = "phone",          required = false) String  phone,
            @RequestParam(value = "email",          required = false) String  email,
            @RequestParam(value = "zipcd",          required = false) String  zipcd,
            @RequestParam(value = "adresa",         required = false) String  adresa,
            @RequestParam(value = "adresb",         required = false) String  adresb,
            @RequestParam(value = "bank_code",      required = false) String  bankCode,
            @RequestParam(value = "bank_account",   required = false) String  bankAccount,
            @RequestParam(value = "account_holder", required = false) String  accountHolder,
            @RequestParam(value = "deduct_day",     required = false) String  deductDay,
            @RequestParam(value = "deduct_amount",  required = false) Long    deductAmount,
            @RequestParam(value = "cycle_type",     required = false) String  cycleType,
            @RequestParam(value = "cycle_months",   required = false) String  cycleMonths,
            @RequestParam(value = "start_date",     required = false) String  startDate,
            @RequestParam(value = "end_date",       required = false) String  endDate,
            @RequestParam(value = "agree_yn",       required = false) String  agreeYn,
            @RequestParam(value = "agree_method",   required = false) String  agreeMethod,
            @RequestParam(value = "status",         required = false) String  status,
            @RequestParam(value = "memo",           required = false) String  memo,
            Authentication auth) {

        User user = (User) auth.getPrincipal();

        Long savedId = cmsMemberService.saveMember(
                id, memberType, memberName, memberNo, idNumber,
                phone, email, zipcd, adresa, adresb,
                bankCode, bankAccount, accountHolder,
                deductDay, deductAmount, cycleType, cycleMonths, startDate, endDate,
                agreeYn, agreeMethod, status, memo,
                user.getUsername());

        AjaxResult result = new AjaxResult();
        if (savedId == null) {
            result.success = false;
            result.message = "저장에 실패했습니다.";
        } else {
            result.data = savedId;
        }
        return result;
    }

    /** 삭제 (soft delete: status = INACTIVE) */
    @DeleteMapping("/{id}")
    public AjaxResult delete(@PathVariable Long id) {
        AjaxResult result = new AjaxResult();
        boolean ok = cmsMemberService.deleteMember(id);
        if (!ok) {
            result.success = false;
            result.message = "삭제에 실패했습니다.";
        }
        return result;
    }
}
