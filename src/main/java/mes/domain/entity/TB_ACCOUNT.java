package mes.domain.entity;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

@Getter
@Setter
@Entity
@NoArgsConstructor
public class TB_ACCOUNT {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)

    private Integer accid;

    private Integer bankid; // 은행 아이디

    private String accnum; // 계좌번호

    private String accname; // 걔좌별칭

    private Integer mijamt; // 지급수수료

    private String refcd; //팝빌 참고코드

    private String onlineid; // 온라인뱅크아이디

    private String onlinepw; // 온라인뱅크비번

    private String accpw; // 결제비번

    private String popsort; // 계좌유형

    private String accbirth; // 생년월일

    private String popyn; //팝빌연동유무
    private String viewid; //조회전용아이디
    private String viewpw; //조회전용비밀번호

}
