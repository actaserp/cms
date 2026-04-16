package mes.domain.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.sql.Date;
import java.sql.Timestamp;

@Entity
@Table(name="Balju")
@NoArgsConstructor
@Data
public class Balju extends AbstractAuditModel{

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "id")
  Integer id;

  @Column(name="\"BaljuHead_id\"")
  Integer BaljuHeadId;

  @Column(name = "\"JumunNumber\"")
  String jumunNumber;

  @Column(name = "\"Material_id\"", nullable = false)
  Integer materialId;

  @Column(name ="\"SujuQty\"", nullable = false)
  Double sujuQty;

  @Column(name ="\"JumunDate\"")
  Date jumunDate;

  @Column(name ="\"DueDate\"")
  Date dueDate;

  @Column(name ="\"CompanyName\"")
  String companyName;

  @Column(name ="\"ProductionPlanDate\"")
  Timestamp productionPlanDate;

  @Column(name ="\"ShipmentPlanDate\"")
  Timestamp shipmentPlanDate;

  @Column(name ="\"Description\"")
  String description;

  @Column(name = "\"AvailableStock\"")
  Float availableStock;

  @Column(name ="\"ReservationStock\"")
  Double reservationStock;

  @Column(name ="\"SujuQty2\"")
  Double sujuQty2;

  @Column(name ="\"UnitPrice\"")
  Double unitPrice;

  @Column(name ="\"Price\"")
  Double price;

  @Column(name ="\"Vat\"")
  Double vat;

  @Column(name ="\"PlanDataPk\"")
  Integer planDataPk;

  @Column(name ="\"PlanTableName\"")
  String planTableName;

  @Column(name ="\"State\"")
  String state;

  @Column(name = "\"ShipmentState\"")
  String shipmentState;

  @Column(name ="\"SujuType\"")
  String sujuType;

  @Column(name = "\"Company_id\"", nullable = false)
  Integer companyId;

  @Column(name="\"_status\"")
  String _status;

  @Column(name = "\"InVatYN\"")
  String inVatYN;

  @Column(name = "\"TotalAmount\"")
  Double TotalAmount;

  @Column(name = "spjangcd")
  String spjangcd;

}
