package mes.domain.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class AccountDto extends BaseDto{

    private Integer id;

    private Integer bankid; // 은행 아이디

    private String accountNumber; // 계좌번호

    private String accountName; // 걔좌별칭

    private Integer mijamt; // 지급수수료

    private String popbillCode; //팝빌 참고코드

    private String onlineid; // 온라인뱅크아이디

    private String onlinepw; // 온라인뱅크비번

    private String accountPassword; // 결제비번

    private String accountType; // 계좌유형

    private String birth; // 생년월일

    private String popyn; //팝빌연동유무
    private String viewid; //조회전용아이디
    private String viewpw; //조회전용비밀번호

}
