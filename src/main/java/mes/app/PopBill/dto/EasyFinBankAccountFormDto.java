package mes.app.PopBill.dto;


import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class EasyFinBankAccountFormDto {

    private Integer accountid;

    private String PopBillId;
    private String PopBillPw;

    private String BankName; //은행코드
    private String AccountNumber; //계좌번호

    private String BankId; //온라인뱅키아이디
    private String BankPw; //온라인뱅킹비번

    private String PaymentPw; //계좌비번
    private String accountType; //계좌유형

    private String identityNumber; //사업자번호 or 생년월일

    private String spjangcd;

    private String CloseType; //정액해지구분
    private String AccountAlias; //계좌별칭
    private String viewid; //조회전용아이디
    private String viewpw; //조회전용비번



}
