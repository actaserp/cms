package mes.domain.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.util.Date;

@Entity
@Table(name="equ_component")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class EquComponent {

    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    int id;

    @Column(name = "cname")
    String cname; // 설비명

    @Column(name = "component")
    String component; //구성품명

    @Column(name = "ctype")
    String ctype; // 부품유형

    @Column(name = "cmodel")
    String cmodel; //모델명

    @Column(name = "cmake")
    String cmake; //제조사

    @Column(name = "camaunt")
    String camaunt; //수량

    @Column(name = "cunit")
    String cunit; //단가

    @Column(name = "cdate")
    Date cdate; // 설비일자

    @Column(name = "cycle")
    String cycle; //교체주기

    @Column(name = "state")
    String state; //상태

    @Column(name = "description")
    String description; //비고

    @Column(name = "equ_id")
    Integer equipmentId; 



}
