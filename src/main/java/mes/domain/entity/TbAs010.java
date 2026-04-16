package mes.domain.entity;

import java.sql.Timestamp;
import javax.persistence.*;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "tb_as010")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper = false)
public class TbAs010 {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "asid")
    private Integer asid; // ID

    @Column(name = "asdate", nullable = false, length = 10)
    private String asdate; // AS 요청일자

    @Column(name = "cltnm", nullable = false, length = 50)
    private String cltnm; // 거래처명

    @Column(name = "userid", nullable = false, length = 50)
    private String userid; // 요청아이디

    @Column(name = "usernm", length = 50)
    private String usernm; // 요청자명

    @Column(name = "asperid", length = 30)
    private String asperid; // 본사담당코드

    @Column(name = "aspernm", length = 30)
    private String aspernm; // 본사담당자명

    @Column(name = "retitle", length = 100)
    private String retitle; // AS 접수제목

    @Column(name = "remark", length = 500)
    private String remark; // AS 요청사항

    @Column(name = "asdv", length = 1)
    private String asdv; // 요청구분

    @Column(name = "asmenu", length = 100)
    private String asmenu; // 화면명

    @Column(name = "recyn", length = 1)
    private String recyn; // 진행구분

    @Column(name = "recperid", length = 30)
    private String recperid; // 접수자코드

    @Column(name = "recpernm", length = 30)
    private String recpernm; // 접수자명

    @Column(name = "recdate")
    private Timestamp recdate; // 접수일자

    @Column(name = "endperid", length = 30)
    private String endperid; // 처리자코드

    @Column(name = "endpernm", length = 30)
    private String endpernm; // 처리자명

    @Column(name = "enddate", length = 10)
    private String enddate; // 처리일자

    @Column(name = "inputdate")
    private Timestamp inputdate; // 입력일자

    @Column(name = "cltcd", length = 30)
    private String cltcd; // 거래처코드

    @Column(name = "as_file")
    private String asFile;
}
