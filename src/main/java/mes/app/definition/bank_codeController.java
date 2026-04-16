package mes.app.definition;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import mes.app.definition.service.bank_codeService;
import mes.domain.entity.BankCode;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BankCodeRepository;

@RestController
@RequestMapping("/api/definition/bank_code")
public class bank_codeController {

    @Autowired
    private bank_codeService bankCodeService;

    @Autowired
    private BankCodeRepository bankCodeRepository;

    // 은행코드 목록 조회
    @GetMapping("/read")
    public AjaxResult getBankCodeList(@RequestParam(value = "bank_name", required = false) String bankName) {
        List<Map<String, Object>> items = this.bankCodeService.getBankCodeList(bankName);
        AjaxResult result = new AjaxResult();
        result.data = items;
        return result;
    }

    // 은행코드 상세 조회
    @GetMapping("/detail")
    public AjaxResult getBankCodeDetail(@RequestParam("id") int id) {
        Optional<BankCode> opt = this.bankCodeRepository.findById(id);
        AjaxResult result = new AjaxResult();
        result.data = opt.orElse(null);
        return result;
    }

    // 팝빌기관코드 리스트 조회 (flag = '0')
    @GetMapping("/popbill_list")
    public AjaxResult getPopbillList() {
        List<Map<String, Object>> items = this.bankCodeService.getPopbillList();
        AjaxResult result = new AjaxResult();
        System.out.println("🚀 popbill_list 조회 결과: " + items.size() + "건"); // 🔥 추가
        result.data = items;
        return result;
    }

    // 참가기관코드 리스트 조회 (flag = '1')
    @GetMapping("/participant_list")
    public AjaxResult getParticipantList() {
        List<Map<String, Object>> items = this.bankCodeService.getParticipantList();
        AjaxResult result = new AjaxResult();
        result.data = items;
        return result;
    }

    // 은행코드 저장 (등록/수정)
    @PostMapping("/save")
    public AjaxResult saveBankCode(
            @RequestParam(value = "id", required = false) Integer id,
            @RequestParam("name") String name, // BANKNM
            @RequestParam("remark") String remark, // 비고(REMARK)
            @RequestParam(value = "bankpopcd", required = false) String bankpopcd, // BANKPOPCD
            @RequestParam(value = "banksubcd", required = false) String banksubcd, // BANKSUBCD
            @RequestParam(value = "bankpopnm", required = false) String bankpopnm,
            @RequestParam(value = "banksubnm", required = false) String banksubnm,
            @RequestParam(value = "bankcd", required = false) String bankcd,
            @RequestParam(value = "subcd", required = false) String subcd,
            Authentication auth
    ) {
        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();

        BankCode bankCode = (id == null) ? new BankCode() : this.bankCodeRepository.getBankCodeById(id);

        boolean nameChk = this.bankCodeRepository.findByName(name).isEmpty();
        if (!name.equals(bankCode.getName()) && !nameChk) {
            result.success = false;
            result.message = "중복된 은행명이 존재합니다.";
            return result;
        }

        bankCode.setName(name);
        bankCode.setRemark(remark);
        bankCode.setBankPopCd(bankpopcd);
        bankCode.setBankSubCd(banksubcd);
        bankCode.setUseYn("1"); // 기본값 사용여부 1로 설정
        bankCode.setBankPopNm(bankpopnm);
        bankCode.setBankSubNm(banksubnm);
        bankCode.setBankcd(bankcd);
        bankCode.setSubcd(subcd);
        //bankCode.set_audit(user); // 생성자/수정자 기록

        this.bankCodeRepository.save(bankCode);
        result.data = bankCode;
        return result;
    }

    // 은행코드 삭제
    @PostMapping("/delete")
    public AjaxResult deleteBankCode(@RequestParam("id") Integer id) {
        this.bankCodeRepository.deleteById(id);
        return new AjaxResult();
    }
}
