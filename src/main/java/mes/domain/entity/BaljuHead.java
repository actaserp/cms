package mes.domain.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.sql.Date;
import java.sql.Timestamp;

@Entity
@Table(name="Balju_head")
@NoArgsConstructor
@Data
public class BaljuHead {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "id")
  Integer id;

  @Column(name = "_status")
  private String _status;

  @Column(name = "_created")
  private Timestamp created;

  @Column(name = "_modified")
  private Timestamp modified;

  @Column(name = "_creater_id")
  private Integer createrId;

  @Column(name = "_modifier_id")
  private Integer modifierId;

  @Column(name = "\"JumunDate\"")
  private Date jumunDate;

  @Column(name = "\"JumunNumber\"")
  private String jumunNumber;

  @Column(name = "\"TotalPrice\"")
  private Double totalPrice;

  @Column(name = "\"ReceivedMoney\"")
  private Double receivedMoney;

  @Column(name = "\"ReceivableMoney\"")
  private Double receivableMoney;

  @Column(name = "\"State\"")
  private String state;

  @Column(name = "\"ShipmentState\"")
  private String shipmentState;

  @Column(name = "\"DeliveryDate\"")
  private Date deliveryDate;

  @Column(name = "\"Company_id\"")
  private Integer companyId;

  @Column(name = "spjangcd")
  private String spjangcd;

  @Column(name = "special_note")
  private String specialNote;

  @Column(name ="\"SujuType\"")
  String sujuType;
}
