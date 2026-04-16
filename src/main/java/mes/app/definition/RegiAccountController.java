package mes.app.definition;

import lombok.extern.slf4j.Slf4j;
import mes.app.aspect.DecryptField;
import mes.app.definition.service.RegiAccountService;
import mes.app.util.UtilClass;
import mes.domain.dto.AccountDto;
import mes.domain.model.AjaxResult;
import org.springframework.web.bind.annotation.*;

@RestController
@Slf4j
@RequestMapping("/api/definition/account")
public class RegiAccountController {

    private final RegiAccountService accountService;

    public RegiAccountController(RegiAccountService accountService) {
        this.accountService = accountService;
    }

    @DecryptField(columns = {"accountNumber", "viewid", "viewpw"}, masks = 0)
    @GetMapping("/read")
    public AjaxResult getRegiAccountList(@RequestParam String bankid,
                                         @RequestParam String accountnum,
                                         @RequestParam String spjangcd
                                         ){

        AjaxResult result = new AjaxResult();

        Integer bankId = UtilClass.parseInteger(bankid);

        result.data = accountService.getAccountList(bankId, accountnum, spjangcd);

        return result;
    }

    @PostMapping("/save")
    public AjaxResult saveAccount(@RequestBody AccountDto accountDto)
    {

        AjaxResult result = new AjaxResult();

        String accnum = accountDto.getAccountNumber().replaceAll("[^0-9]", ""); // 하이픈 등 제거
        if (!isValidAccountNumber(accnum)) {
            result.success = false;
            result.message = "계좌번호 형식이 유효하지 않습니다.";
            return result;
        }
        accountDto.setAccountNumber(accnum);

        accountService.saveAccount(accountDto);

        result.success = true;
        result.message = "저장되었습니다.";

        return result;
    }


    @PostMapping("/delete")
    public AjaxResult deleteAccount(@RequestParam Integer id
    ){

        AjaxResult result = new AjaxResult();

        accountService.deleteAccount(id);

        result.success = true;
        result.message = "삭제되었습니다.";

        return result;
    }

    public static boolean isValidAccountNumber(String accnum) {
        if (accnum == null) return false;

        // 숫자만 포함 & 10~14자리 사이
        return accnum.matches("^\\d{10,14}$");
    }
}
