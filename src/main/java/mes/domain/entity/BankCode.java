package mes.domain.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.springframework.web.bind.annotation.RequestParam;

@Entity
@Table(name = "tb_xbank")  // 테이블 이름 주의
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper = false)
public class BankCode {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "bankid") // 은행 ID
    private Integer id;

    @Column(name = "banknm")
    private String name;  // 은행명

    @Column(name = "bankpopcd")
    private String bankPopCd;  // 팝빌기관코드

    @Column(name = "bankpopnm")
    private String bankPopNm;  // 팝빌기관코드명

    @Column(name = "banksubcd")
    private String bankSubCd;  // 참가기관코드

    @Column(name = "banksubnm")
    private String bankSubNm;  // 참가기관코드명

    @Column(name = "remark")
    private String remark;  // 비고

    @Column(name = "useyn")
    private String useYn;  // 사용여부

    @Column(name = "bankcd")
    private String bankcd;  // 은행코드

    @Column(name = "subcd")
    private String subcd;  // 지점코드
}
