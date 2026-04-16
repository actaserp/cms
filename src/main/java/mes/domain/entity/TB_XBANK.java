package mes.domain.entity;

import lombok.*;

import javax.persistence.*;

@Entity
@Table(name = "tb_xbank")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TB_XBANK {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "bankid")
    private Integer bankId; // 은행ID

    @Column(name = "banknm", length = 50)
    private String bankNm; // 은행명

    @Column(name = "bankpopcd", length = 10)
    private String bankPopCd; // 팝빌관리코드

    @Column(name = "bankpopnm", length = 50)
    private String bankPopNm; // 팝빌관리코드명

    @Column(name = "banksubcd", length = 10)
    private String bankSubCd; // 기관관리코드

    @Column(name = "banksubnm", length = 50)
    private String bankSubNm; // 기관관리코드명

    @Column(name = "remark", length = 50)
    private String remark; // 비고

    @Column(name = "useyn", length = 1)
    private String useYn; // 사용여부

    @Column(name = "spjangcd", length = 2)
    private String spjangCd = "ZZ"; // 사업장코드 (기본 ZZ)

    @Column(name = "vercode", length = 50)
    private String verCode; // 버전 코드
}
