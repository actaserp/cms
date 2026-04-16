package mes.domain.entity;

import java.sql.Timestamp;
import javax.persistence.*;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "tb_as011")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper = false)
public class TbAs011 {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "fixid")
    private Integer fixid; // ID

    @Column(name = "asid", nullable = false)
    private Integer asid; // ASID (TB_AS010 FK 가능)

    @Column(name = "fixdate", nullable = false, length = 10)
    private String fixdate; // 처리일자

    @Column(name = "asperid", length = 30)
    private String asperid; // 처리자코드

    @Column(name = "aspernm", length = 30)
    private String aspernm; // 처리자명

    @Column(name = "remark", length = 500)
    private String remark; // 처리내용

    @Column(name = "inputdate")
    private Timestamp inputdate; // 입력일자

    @Column(name = "as_file")
    private String asFile;
}
