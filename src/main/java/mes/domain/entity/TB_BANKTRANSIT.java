package mes.domain.entity;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;

@Entity
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_banktransit")
public class TB_BANKTRANSIT extends BaseEntity{

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer ioid;

    @Column(length = 1)
    private String ioflag; //입출금 구분

    @Column(length = 50)
    private String tid; //거래내역 id

    @Column(length = 8)
    private String trdate; //거래일자

    private Integer trserial; //거래일련번호

    @Column(length = 20)
    private String trdt; //거래일시

    private Integer accin; //입금액
    private Integer accout; //출금액
    private Integer balance; //잔액

    @Column(length = 500)
    private String remark1; //적요1

    @Column(length = 500)
    private String remark2; //적요1

    @Column(length = 500)
    private String remark3;//적요1

    @Column(length = 500)
    private String remark4;//적요1

    @Column(length = 20)
    private String regdt;//등록일시

    @Column(length = 20)
    private String regpernm; //등록자

    @Column(length = 100)
    private String memo; //메모

    @Column(length = 30)
    private String jobid; //작업아이디

    private Integer cltcd; //거래처 아이디
    private Integer trid; //거래구분 id

    @Column(length = 1)
    private String iotype; //입금형태

    @Column(length = 50)
    private String banknm; //은행명

    @Column(length = 50)
    private String accnum; //계좌번호

    private Integer accid; //계좌 아이디

    @Column(length = 50)
    private String etcremark; //기타구분

    @Column(length = 50)
    private String eumnum; //전자어음번호

    @Column(length = 8)
    private String eumfrdt; //발행일

    @Column(length = 8)
    private String eumtodt; //만료일

    private Integer feeamt; //수수료

    @Column(length = 1)
    private String feeflag; //수수료여부

    @Column(length = 8)
    private String acccd; //계정코드

    @Column(length = 1)
    private String cltflag; //거래처 구분값

    @Column(length = 50)
    private String projno; //프로젝트 관리




}
