package mes.domain.dto;


import lombok.*;
import mes.app.util.UtilClass;
import mes.domain.entity.TB_BANKTRANSIT;
import org.springframework.util.StringUtils;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;


@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BankTransitDto {

    private Integer bankTransitId; // 입출금 고유 ID

    @NotBlank(message = "입출금 구분은 필수입니다.")
    private String inoutFlag; // 입출금 구분

    private String transactionId; // 거래내역 ID
    private String transactionDate; // 거래일자
    private String transactionHour;

    private Integer transactionSerial; // 거래일련번호
    private String transactionDatetime; // 거래일시
    private Integer depositAmount; // 입금액
    private Integer withdrawalAmount; // 출금액
    private Integer balanceAmount; // 잔액
    private String commission; // 수수료
    private String note1; // 적요1
    private String note2; // 적요2
    private String note3; // 적요3
    private String note4; // 적요4
    private String regidate; // 등록일시
    private String registername; // 등록자
    private String memo; // 메모
    private String jobId; // 작업 아이디
    private Integer clientId; // 거래처 ID
    private Integer transactionTypeId; // 거래구분 ID
    private String depositAndWithdrawalType; // 입출금 형태
    private String spjangcd; // 사업장

    private String bankName; // 은행명

    private String accountNumber; // 계좌번호

    private Integer accountId; // 계좌 아이디

    private String etc; // 기타 구분

    @Pattern(regexp = "^$|^\\d+$", message = "숫자만 가능합니다.")
    private String bill; // 전자어음 번호

    private String promissoryNoteIssueDate; // 전자어음 발행일

    private String expiration; // 전자어음 만기일

    private String feeFlag; // 수수료 여부
    private String accountCode; // 계정 코드

    @NotBlank(message = "금액은 필수입니다.")
    @Pattern(regexp = "^[\\d,]+$", message = "숫자와 쉼표만 입력가능합니다.")
    private String money;

    private String projectNumber; //프로젝트이름
    private String projno; //프로젝트번호

    private String clientFlag; //거래처 구분값

    private String clientName;

    public static TB_BANKTRANSIT toEntity(BankTransitDto dto, TB_BANKTRANSIT banktransit){

        if(dto == null) return null;

        String expiration = String.valueOf(dto.getExpiration());
        if(!expiration.isEmpty()){
            if(!UtilClass.isValidDate(expiration)){
                throw new IllegalArgumentException("유효하지 않은 날짜입니다.");
            }
        }

        String inoutFlag = dto.getInoutFlag();
        Integer accin = 0;
        Integer accout = 0;
        String money = dto.getMoney().replaceAll(",", "");
        String commission = dto.getCommission().replaceAll(",", "");


        if(inoutFlag.equals("0")){
            accin = UtilClass.parseInteger(money);
        }else if(inoutFlag.equals("1")){
            accout = UtilClass.parseInteger(money);
        }

        String accountNum = dto.getAccountNumber() != null ? dto.getAccountNumber().replaceAll("-", "") : null;

        banktransit.setIoid(dto.getBankTransitId());
        banktransit.setIoflag(dto.getInoutFlag());  //입출금구분
        banktransit.setTrdate(dto.getTransactionDate().replaceAll("-", ""));
        banktransit.setTrdt(UtilClass.combineDateAndHourReturnyyyyMMddHHmmss(dto.getTransactionDate(), dto.getTransactionHour()));
        banktransit.setAccin(accin);
        banktransit.setAccout(accout);
        banktransit.setRemark1(dto.getNote1());
        banktransit.setRegdt(UtilClass.combineDateAndHourReturnyyyyMMddHHmmss(dto.getRegidate(), null));
        banktransit.setRegpernm(dto.getRegistername());

        banktransit.setTrid(UtilClass.parseInteger(dto.getTransactionTypeId()));
        banktransit.setIotype(dto.getDepositAndWithdrawalType());
        banktransit.setBanknm(dto.getBankName());
        banktransit.setFeeamt(UtilClass.parseInteger(commission));
        banktransit.setAccnum(accountNum);

        banktransit.setEumnum(dto.getBill());
        banktransit.setEumtodt(dto.getExpiration());
        banktransit.setEtcremark(dto.getEtc());
        banktransit.setMemo(dto.getMemo());
        banktransit.setSpjangcd(dto.getSpjangcd());

        banktransit.setProjno(StringUtils.hasText(dto.getProjectNumber()) ? dto.getProjno() : null);
        banktransit.setAccid(StringUtils.hasText(accountNum) ? dto.getAccountId() : null);

        boolean hasClient = StringUtils.hasText(dto.getClientName());
        banktransit.setCltcd(hasClient ? UtilClass.parseInteger(dto.getClientId()) : null);
        banktransit.setCltflag(hasClient ? dto.getClientFlag() : null);


        return banktransit;
    }
}
