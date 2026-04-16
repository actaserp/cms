package mes.domain.entity.approval;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name="TB_BBSINFO")
@Setter
@Getter
@NoArgsConstructor
public class TB_BBSINFO {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column
    Integer BBSSEQ;

    @Column
    String BBSDATE;
    @Column
    String BBSSUBJECT;
    @Column
    String BBSUSER;
    @Column
    String BBSTEXT;
    @Column
    String BBSTEL;
    @Column
    String BBSFRDATE;
    @Column
    String BBSTODATE;
    @Column(updatable = false)
    LocalDateTime INDATEM;
    @Column
    String INUSERID;
}
