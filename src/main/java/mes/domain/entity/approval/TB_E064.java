package mes.domain.entity.approval;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "tb_e064") // DB 테이블명과 매핑
@Setter
@Getter
@NoArgsConstructor
public class TB_E064 {

    @EmbeddedId
    private TB_E064_PK id;

    @Column(length = 3)
    private String seq;

    @Column
    private Integer kcpersonid;

    @Column(length = 2)
    private String gubun;

    @Column(length = 30)
    private String remark;

    @Column(length = 1)
    private String kcchk;

    @Column
    private Integer inperid;

    @Column(length = 8)
    private String indate;

}
