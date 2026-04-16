package mes.app.PopBill;

import com.popbill.api.PopbillException;
import com.popbill.api.Response;
import com.popbill.api.TaxinvoiceService;
import mes.app.util.UtilClass;
import mes.domain.model.AjaxResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpSession;

@RestController
@RequestMapping("/BaseService")
public class BaseServiceServiceController {

    @Autowired
    private TaxinvoiceService taxinvoiceService;

    @Value("${popbill.linkId}")
    private String LinkId;

    /*@RequestMapping(value = "checkIsMember", method = RequestMethod.GET)
    public String checkIsMember(@RequestParam String corpNum) throws PopbillException {
        *//**
         * 사업자번호를 조회하여 연동회원 가입여부를 확인합니다.
         * - LinkID는 연동신청 시 팝빌에서 발급받은 링크아이디 값입니다.
         * - https://developers.popbill.com/reference/taxinvoice/java/api/member#CheckIsMember
         *//*

        AjaxResult result = new AjaxResult();


        // 조회할 사업자번호, '-' 제외 10자리

        try {
            Response response = taxinvoiceService.checkIsMember(corpNum, LinkId);
            System.out.println("");
        } catch (PopbillException e) {
            return "exception";
        }

        return "response";
    }*/

    @RequestMapping(value = "checkIsMember", method = RequestMethod.GET)
    public AjaxResult checkIsMember(@RequestParam String spjangcd, HttpSession session) throws PopbillException {
        /**
         * 사업자번호를 조회하여 연동회원 가입여부를 확인합니다.
         * - LinkID는 연동신청 시 팝빌에서 발급받은 링크아이디 값입니다.
         * - https://developers.popbill.com/reference/taxinvoice/java/api/member#CheckIsMember
         */

        AjaxResult result = new AjaxResult();
        result.success = false;

        try {
            String corpNum = UtilClass.getsaupnumInfoFromSession(spjangcd, session);

            Response response = taxinvoiceService.checkIsMember(corpNum, LinkId);

            if(response != null){
                long code = response.getCode();
                String message = response.getMessage();
                result.success = true;

                if(code == 1 && message.equals("가입")){
                    result.message = "연동회원에 등록되어 있습니다.";
                }else{
                    result.message = "연동회원에 등록되어 있지 않습니다.";
                }
                return result;
            }else{
                result.message = "API 통신 오류 발생";
                return result;
            }

        } catch (PopbillException e) {

            result.message = e.getMessage();
        }

        return result;
    }


    @RequestMapping(value = "getBalance", method = RequestMethod.GET)
    public String getBalance(Model m) throws PopbillException {
        /**
         * 연동회원의 잔여포인트를 확인합니다.
         * - 과금방식이 파트너과금인 경우 파트너 잔여포인트 확인(GetPartnerBalance API) 함수를 통해 확인하시기 바랍니다.
         * - https://developers.popbill.com/reference/taxinvoice/java/api/point#GetBalance
         */
        String CorpNum = "1778602466";
        try {
            double remainPoint = taxinvoiceService.getBalance(CorpNum);

            m.addAttribute("Result", remainPoint);

        } catch (PopbillException e) {
            m.addAttribute("Exception", e);
            return "exception";
        }

        return "result";
    }
}
