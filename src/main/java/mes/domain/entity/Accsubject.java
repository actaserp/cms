package mes.domain.entity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Entity
@Table(name="tb_accsubject")
@NoArgsConstructor
@Data
@EqualsAndHashCode( callSuper=false)
public class Accsubject {

    @Id
    @Column(name = "acccd")
    String acccd;

    @Column(name = "accnm")
    String accnm; // 계정명

    @Column(name = "accprtnm")
    String accprtnm; //양식명

    @Column(name = "uacccd")
    String uacccd; //상위계정

    @Column(name = "acclv")
    Integer acclv; //레벨

    @Column(name = "drcr")
    String drcr; //차대

    @Column(name = "dcpl")
    String dcpl; //대손

    @Column(name = "spyn")
    String spyn; //전표사용

    @Column(name = "useyn")
    String useyn; //사용여부

    @Column(name = "cacccd")
    String cacccd; //차감계정

    @Column(name = "etccode")
    String etccode; //연결코드

    @Column(name = "spjangcd")
    String spjangcd;


}
