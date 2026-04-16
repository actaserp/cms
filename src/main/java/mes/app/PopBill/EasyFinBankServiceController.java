/**
  * 팝빌 계좌조회 API Java SDK SpringBoot Example
  *
  * SpringBoot 연동 튜토리얼 안내 : https://developers.popbill.com/guide/easyfinbank/java/getting-started/tutorial?fwn=springboot
  * 연동 기술지원 연락처 : 1600-9854
  * 연동 기술지원 이메일 : code@linkhubcorp.com
  *
  */
package mes.app.PopBill;

import com.popbill.api.*;
import com.popbill.api.easyfin.*;
import lombok.extern.slf4j.Slf4j;
import mes.Encryption.EncryptionKeyProvider;
import mes.Encryption.EncryptionUtil;
import mes.app.PopBill.dto.EasyFinBankAccountFormDto;
import mes.app.PopBill.enums.BankJobState;
import mes.app.PopBill.service.EasyFinBankCustomService;
import mes.app.util.UtilClass;
import mes.domain.entity.TB_ACCOUNT;
import mes.domain.model.AjaxResult;
import mes.domain.repository.TB_ACCOUNTRepository;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import javax.crypto.IllegalBlockSizeException;
import javax.servlet.http.HttpSession;
import java.util.*;
import java.util.function.Function;

@Slf4j
@RestController
@RequestMapping("EasyFinBankService")
public class EasyFinBankServiceController {

    @Autowired
    private EasyFinBankService easyFinBankService;

    @Autowired
    private EasyFinBankCustomService easyFinBankCustomService;

    @Autowired
    private TB_ACCOUNTRepository accountRepository;


    @RequestMapping(value = "listBankAccount", method = RequestMethod.GET)
    public String listBankAccount(Model m) {
        /**
         * 팝빌에 등록된 계좌정보 목록을 반환합니다.
         * - https://developers.popbill.com/reference/easyfinbank/java/api/manage#ListBankAccount
         */
        try {
            String CorpNum = "1778602466";

            EasyFinBankAccount[] bankList = easyFinBankService.listBankAccount(CorpNum);
            m.addAttribute("BankAccountList", bankList);

        } catch (PopbillException e) {
            m.addAttribute("Exception", e);
            return "exception";
        }

        return "EasyFinBank/ListBankAccount";
    }



    //TODO : 조회전용 계정이 경우에 따라 필요하다. 신한은행이나 신협중앙회면 필요함.

    @RequestMapping(value = "registBankAccount", method = RequestMethod.POST)
    public AjaxResult registBankAccount(EasyFinBankAccountFormDto form, HttpSession session) throws Exception {

        AjaxResult result = new AjaxResult();

        Optional<String> validationMessage = validate(form);
        if(validationMessage.isPresent()){
            return fail(result, validationMessage.get());
        }

        String bankName = form.getBankName();

        /**
         * BankID -> 국민은행일 경우 인터넷뱅킹 아이디가 필수임 그래서 체크함.
         * FastID, FastPWD -> 아이엠뱅크, 신한은행, 신협중앙회 일 경우 조회전용계정필수임.
         * **/


        EasyFinBankAccountForm bankInfo = new EasyFinBankAccountForm();

        TB_ACCOUNT acc = Optional.ofNullable(form.getAccountid())
                        .map(accountRepository::findById)
                        .flatMap(Function.identity())
                        .orElseGet(TB_ACCOUNT::new);

        String accountpw = resolvePassword(form.getPaymentPw(), acc.getAccpw());
        form.setPaymentPw(accountpw);

        String viewpw = resolvePassword(form.getViewpw(), acc.getViewpw());
        form.setViewpw(viewpw);

        if(!validateBankRequirement(bankName, form, result, bankInfo)){
            return result;
        }

        String decryptedAccountNum = (acc.getAccnum() != null)
                ? EncryptionUtil.decrypt(acc.getAccnum())
                : form.getAccountNumber();

        try{


            bankInfo.setBankCode(form.getBankName());
            bankInfo.setAccountNumber(decryptedAccountNum);

            try{
                bankInfo.setAccountPWD(EncryptionUtil.decrypt(form.getPaymentPw()));
            }catch(IllegalBlockSizeException e){
                bankInfo.setAccountPWD(form.getPaymentPw());
            }

            bankInfo.setAccountType(form.getAccountType());
            bankInfo.setIdentityNumber(form.getIdentityNumber().replaceAll("-", ""));
            bankInfo.setAccountName(form.getAccountAlias());

            String CorpNum = UtilClass.getsaupnumInfoFromSession(form.getSpjangcd(), session);


            Response response = easyFinBankService.registBankAccount(CorpNum, bankInfo);
            //Response response = new Response();


            if(response.getCode() == 1){

                if(form.getAccountid() != null){
                    easyFinBankCustomService.saveRegistAccount(form, acc);
                }
                result.success = true;
                result.message = response.getMessage();

            }else{
                result.success = false;
                result.message = response.getMessage();
            }

        } catch (Exception e){
            log.error(e.getMessage());
            result.success = false;
            result.message = e.getMessage();
        }
        return result;
    }


    @RequestMapping(value = "requestJob", method = RequestMethod.POST)
    public AjaxResult requestJob(@RequestParam String frdate,
                                 @RequestParam String todate,
                                 @RequestParam String managementnum,
                                 @RequestParam Integer accountid,
                                 @RequestParam String spjangcd,
                                 @RequestParam String bankname, HttpSession session) {
        /**
         * 계좌 거래내역을 확인하기 위해 팝빌에 수집요청을 합니다. (조회기간 단위 : 최대 1개월)
         * - 조회일로부터 최대 3개월 이전 내역까지 조회할 수 있습니다.
         * - https://developers.popbill.com/reference/easyfinbank/java/api/job#RequestJob
         */
        AjaxResult result  = new AjaxResult();
        result.success = false;

        TB_ACCOUNT acc = accountRepository.findById(accountid).orElseThrow(() ->{
            throw new IllegalArgumentException("올바른 계좌가 아닙니다.");
        });

        String accountnumber = acc.getAccnum();

        //계좌번호, 은행코드 없으면 리턴
        if(!validateRequest(accountnumber, managementnum, result)) return result;

        frdate = frdate.replaceAll("-", "");
        todate = todate.replaceAll("-", "");


        try {
            accountnumber = EncryptionUtil.decrypt(accountnumber, EncryptionKeyProvider.getKey());


            String CorpNum = UtilClass.getsaupnumInfoFromSession(spjangcd, session);
            String jobID = easyFinBankService.requestJob(CorpNum, managementnum, accountnumber, frdate, todate);

            String jobState = waitForJobComplete(CorpNum, jobID);

            if(!jobState.equals(BankJobState.COMPLETE.getCode())){
                result.message = jobState;
                return result;
            }

            EasyFinBankSearchResult searchInfo = easyFinBankService.search(CorpNum, jobID, null, null, null, null, null);

            if(searchInfo.getCode() != 1){
                result.message = searchInfo.getMessage();
                return result;
            }

            List<EasyFinBankSearchDetail> list = searchInfo.getList();
            List<Map<String, Object>> mapList = easyFinBankCustomService.convertToMapList(list);

            //비동기로 DB에 내역 저장

            easyFinBankCustomService.saveBankDataAsync(list, jobID, accountnumber, accountid, bankname, spjangcd);

            System.out.println(mapList);

            result.success = true;
            result.data = mapList;


        } catch (Exception e) {
            log.error("에러발생 : {}", e.getMessage());
            result.message = e.getMessage();
            return result;
        }

        return result;
    }

    @RequestMapping(value = "getBankAccountInfo", method = RequestMethod.GET)
    public AjaxResult getBankAccountInfo(EasyFinBankAccountFormDto form, HttpSession session){
        /**
         * 팝빌에 등록된 계좌 정보를 확인합니다.
         * - https://developers.popbill.com/reference/easyfinbank/java/api/manage#GetBankAccountInfo
         */
        AjaxResult result = new AjaxResult();

        // 기관코드
        // 산업은행-0002 / 기업은행-0003 / 국민은행-0004 /수협은행-0007 / 농협은행-0011 / 우리은행-0020
        // SC은행-0023 / 대구은행-0031 / 부산은행-0032 / 광주은행-0034 / 제주은행-0035 / 전북은행-0037
        // 경남은행-0039 / 새마을금고-0045 / 신협은행-0048 / 우체국-0071 / KEB하나은행-0081 / 신한은행-0088 /씨티은행-0027
        String BankCode = form.getBankName();

        // 계좌번호 하이픈('-') 제외


        try {
            TB_ACCOUNT acc = accountRepository.findById(form.getAccountid())
                    .orElseThrow(() -> new IllegalArgumentException("올바른 계좌가 아닙니다."));

            String AccountNumber = EncryptionUtil.decrypt(acc.getAccnum());

            String CorpNum = UtilClass.getsaupnumInfoFromSession(form.getSpjangcd(), session);

            EasyFinBankAccount bankAccountInfo = easyFinBankService.getBankAccountInfo(CorpNum.replaceAll("-", ""), BankCode, AccountNumber);
            log.info("계좌정보 객체 : {}", bankAccountInfo);
            result.success = true;

            if(bankAccountInfo.getContractState() == 1){

            }else if(bankAccountInfo.getContractState() == 1){

            }

        } catch (IllegalArgumentException | PopbillException e) {
            result.success = false;
            result.message = e.getMessage();
        } catch (Exception e) {
            throw new RuntimeException("복호화에 실패하였습니다. (팝빌 계좌해지 관련)", e);
        }

        return result;
    }

    @RequestMapping(value = "closeBankAccount", method = RequestMethod.GET)
    public AjaxResult closeBankAccount(EasyFinBankAccountFormDto form, HttpSession session) {
        /**
         * 계좌의 정액제 해지를 요청합니다.
         * - https://developers.popbill.com/reference/easyfinbank/java/api/manage#CloseBankAccount
         */
        AjaxResult result = new AjaxResult();
        // 기관코드
        // 산업은행-0002 / 기업은행-0003 / 국민은행-0004 /수협은행-0007 / 농협은행-0011 / 우리은행-0020
        // SC은행-0023 / 대구은행-0031 / 부산은행-0032 / 광주은행-0034 / 제주은행-0035 / 전북은행-0037
        // 경남은행-0039 / 새마을금고-0045 / 신협은행-0048 / 우체국-0071 / KEB하나은행-0081 / 신한은행-0088 /씨티은행-0027
        String BankCode = form.getBankName();

        // 해지유형, "일반", "중도" 중 택 1
        // 일반(일반해지) – 이용중인 정액제 기간 만료 후 해지
        // 중도(중도해지) – 해지 요청일 기준으로 정지되고 팝빌 담당자가 승인시 해지
        // └ 중도일 경우, 정액제 잔여기간은 일할로 계산되어 포인트 환불 (무료 이용기간 중 해지하면 전액 환불)
        String CloseType = form.getCloseType();

        try {
            TB_ACCOUNT acc = accountRepository.findById(form.getAccountid())
                    .orElseThrow(() -> new IllegalArgumentException("올바른 계좌가 아닙니다."));

            String AccountNumber = EncryptionUtil.decrypt(acc.getAccnum());

            String CorpNum = UtilClass.getsaupnumInfoFromSession(form.getSpjangcd(), session);

            Response response = easyFinBankService.closeBankAccount(CorpNum, BankCode, AccountNumber, CloseType);

            //Response response = null;

            log.info("해제신청api 리턴객체 : {}", response);

            if(response.getCode() == 1){
                result.success = true;
                acc.setPopyn("0");
                accountRepository.save(acc);
            }else{
                result.success = false;
            }
            result.message = response.getMessage();


        } catch (PopbillException e) {
            result.success = false;
            result.message = e.getMessage();
            return result;

        } catch (IllegalArgumentException e) {
            result.success = false;
            result.message = e.getMessage(); // 계좌가 없음
            return result;

        } catch (Exception e) {
            // 복호화, 기타 오류 → 실제로 터뜨리기 (result는 의미 없음)
            throw new RuntimeException("복호화 또는 팝빌 계좌 해지 요청 중 오류 발생", e);
        }

        return result;

    }
    @Deprecated
    @RequestMapping(value = "revokeCloseBankAccount", method = RequestMethod.GET)
    public String revokeCloseBankAccount(Model m) {
        /**
         * 신청한 정액제 해지요청을 취소합니다.
         * - https://developers.popbill.com/reference/easyfinbank/java/api/manage#RevokeCloseBankAccount
         */

        // 기관코드
        // 산업은행-0002 / 기업은행-0003 / 국민은행-0004 /수협은행-0007 / 농협은행-0011 / 우리은행-0020
        // SC은행-0023 / 대구은행-0031 / 부산은행-0032 / 광주은행-0034 / 제주은행-0035 / 전북은행-0037
        // 경남은행-0039 / 새마을금고-0045 / 신협은행-0048 / 우체국-0071 / KEB하나은행-0081 / 신한은행-0088 /씨티은행-0027
        String BankCode = "0004";

        // 계좌번호 하이픈('-') 제외
        String AccountNumber = "94160200188218";

        try {
            String CorpNum = "1778602466";
            Response response = easyFinBankService.revokeCloseBankAccount(CorpNum, BankCode, AccountNumber);

            m.addAttribute("Response", response);

        } catch (PopbillException e) {
            m.addAttribute("Exception", e);
            return "exception";
        }

        return "response";
    }

    @RequestMapping(value = "updateBankAccount", method = RequestMethod.GET)
    public AjaxResult updateBankAccount(EasyFinBankAccountFormDto form, HttpSession session)  {
        /**
         * 팝빌에 등록된 계좌정보를 수정합니다.
         * - https://developers.popbill.com/reference/easyfinbank/java/api/manage#UpdateBankAccount
         */
        AjaxResult result = new AjaxResult();

        if (StringUtils.isEmpty(form.getBankName()) ||
                StringUtils.isEmpty(form.getAccountNumber()) ||
                StringUtils.isEmpty(form.getPaymentPw())) {
            return fail(result, "은행과 계좌번호와 계좌비번은 필수값입니다.");
        }

        // 기관코드
        // 산업은행-0002 / 기업은행-0003 / 국민은행-0004 /수협은행-0007 / 농협은행-0011 / 우리은행-0020
        // SC은행-0023 / 대구은행-0031 / 부산은행-0032 / 광주은행-0034 / 제주은행-0035 / 전북은행-0037
        // 경남은행-0039 / 새마을금고-0045 / 신협은행-0048 / 우체국-0071 / KEB하나은행-0081 / 신한은행-0088 /씨티은행-0027

        try {
            TB_ACCOUNT account = accountRepository.findById(form.getAccountid())
                    .orElseThrow(() -> new IllegalArgumentException("존재하지 않는 계좌입니다."));

            UpdateEasyFinBankAccountForm edit = new UpdateEasyFinBankAccountForm();

            String CorpNum = UtilClass.getsaupnumInfoFromSession(form.getSpjangcd(), session);
            String BankCode = form.getBankName();

            String accpw = UtilClass.getStringSafe(account.getAccpw());
            edit.setAccountPWD(resolvePassword(form.getPaymentPw(), EncryptionUtil.decrypt(accpw)));
            edit.setAccountName(form.getAccountAlias());

            switch (BankCode) {
                case "0031": // 기업
                case "0088": // 신한
                case "0048": // 신협
                    if (StringUtils.isEmpty(form.getViewid()) || StringUtils.isEmpty(form.getViewpw())) {
                        return fail(result, "해당 은행은 조회전용계정이 필수입니다.");
                    }
                    edit.setFastID(form.getViewid());
                    String viewpw = UtilClass.getStringSafe(account.getViewpw());
                    edit.setFastPWD(resolvePassword(form.getViewpw(), EncryptionUtil.decrypt(viewpw)));

                    break;

                case "0004": // 국민
                    if (StringUtils.isEmpty(form.getBankId())) {
                        return fail(result, "해당 은행은 인터넷뱅킹아이디가 필수입니다.");
                    }
                    edit.setBankID(form.getBankId());
                    break;

                default:
                    // 특별 처리 없음 또는 무시
                    break;
            }


            String AccountNumber = account.getAccnum();
            String plainAccnum = EncryptionUtil.decrypt(AccountNumber);

            /*if(1==1){
                return result;
            }*/
            Response response = easyFinBankService.updateBankAccount(CorpNum, BankCode, plainAccnum, edit, null);
            //Response response = null;

            if(response.getCode() == 1){
                account.setAccpw(EncryptionUtil.encrypt(edit.getAccountPWD()));
                account.setOnlineid(edit.getBankID());
                account.setAccname(edit.getAccountName());
                account.setViewid(edit.getFastID());

                if(StringUtils.isEmpty(edit.getFastPWD())){
                    account.setViewpw(EncryptionUtil.encrypt(edit.getFastPWD()));
                }
                accountRepository.save(account);
            }

            result.message = response.getMessage();

        } catch (PopbillException e) {
            result.success = false;
            result.message = e.getMessage();
        }catch (Exception e) {
            // 복호화, 기타 오류 → 실제로 터뜨리기 (result는 의미 없음)
            throw new RuntimeException("복호화 또는 팝빌 계좌 해지 요청 중 오류 발생", e);
        }
        // 계좌정보 클래스 인스턴스 생성

        return result;

    }

    private boolean validateBankRequirement(String bankName, EasyFinBankAccountFormDto form, AjaxResult result, EasyFinBankAccountForm bankInfo){
        switch (bankName) {
            case "0004": // 국민은행
                String bankId = form.getBankId();

                if (bankId == null || bankId.isEmpty()) {
                    result.success = false;
                    result.message = "국민은행은 인터넷뱅킹 아이디가 필수입니다.";
                    return false;
                }

                bankInfo.setBankID(form.getBankId());
                break;

            case "0031": // 아이엠뱅크
            case "0088": // 신한은행
            case "0048": // 신협중앙회
                String viewid = form.getViewid();
                String viewpw = form.getViewpw();

                if(StringUtils.isEmpty(viewid) || StringUtils.isEmpty(viewpw)){
                    result.success = false;
                    result.message = "아이엠뱅크, 신한은행, 신협중앙회는 조회전용 계정이 필수입니다.";
                    return false;
                }

                bankInfo.setFastID(viewid);
                try {
                    bankInfo.setFastPWD(EncryptionUtil.decrypt(viewpw));
                } catch (Exception e) {
                    log.error("복호화 실패 {} , 실패한 문자열 : {}", e.getMessage(), viewpw);
                    bankInfo.setFastPWD(viewpw);
                }
                break;
            default:
                break;
        }
        return true;
    }

    public Optional<String> validate(EasyFinBankAccountFormDto form){
        if(form == null) {
            return Optional.of("요청 폼이 비어있습니다.");
        }
        if(StringUtils.isEmpty(form.getBankName())){
            return Optional.of("은행코드가 비어있습니다.");
        }
        if(StringUtils.isEmpty(form.getAccountNumber())){
            return Optional.of("계좌번호가 비어있습니다.");
        }
        if(StringUtils.isEmpty(form.getPaymentPw())){
            return Optional.of("계좌 비밀번호가 비어있습니다.");
        }
        if(StringUtils.isEmpty(form.getAccountType())){
            return Optional.of("계좌 유형이 비어있습니다.");
        }
        if(StringUtils.isEmpty(form.getIdentityNumber())){
            return Optional.of("사업자번호(혹은 생년월일)가 비어있습니다.");
        }
        return  Optional.empty();
    }
    private boolean validateRequest(String accountnumber, String managementnum, AjaxResult result){
        if(StringUtils.isEmpty(accountnumber) || StringUtils.isEmpty(managementnum)){
            fail(result, "관리코드 및 계좌번호가 누락되었습니다.");
            return false;
        }
        return true;
    }


    private AjaxResult fail(AjaxResult result, String message) {
        result.success = false;
        result.message = message;
        return result;
    }

    public String waitForJobComplete(String CorpNum, String jobId) throws InterruptedException, PopbillException {
        int maxRetry = 10;
        int inteval = 1000;

        for(int i=0; i < maxRetry; i++){
            EasyFinBankJobState jobState = easyFinBankService.getJobState(CorpNum, jobId);
            String jobStateCode = jobState.getJobState(); // 1=대기, 2=진행중, 3=완료
            long errorCode = jobState.getErrorCode();

            if(errorCode != 1 && errorCode != 0){
                log.info("에러코드 발생 {}" ,errorCode);
                return "에러발생";
            }

            BankJobState state = BankJobState.fromCode(jobStateCode);

            if(state == BankJobState.COMPLETE){
                log.info("수집완료");
                return BankJobState.COMPLETE.getCode();
            }

            Thread.sleep(inteval);
        }
        return BankJobState.TIMEOUT.getCode();
    }

    private String resolvePassword(String input, String original) {
        return input.contains("⋆") ? original : input;
    }



}