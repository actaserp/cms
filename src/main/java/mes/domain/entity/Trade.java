package mes.domain.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Entity
@Table(name="tb_trade")
@NoArgsConstructor
@Data
@EqualsAndHashCode( callSuper=false)
public class Trade {

    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    @Column(name = "trid")
    Integer id;

    @Column(name = "ioflag")
    String ioflag; // 입출구분

    @Column(name = "tradenm")
    String tradenm; //거래구분명

    @Column(name = "acccd")
    String acccd; //계정코드

    @Column(name = "reacccd")
    String reacccd; //상대계정

    @Column(name = "remark")
    String remark; //비고

    @Column(name="spjangcd")
    String spjangcd;

}
