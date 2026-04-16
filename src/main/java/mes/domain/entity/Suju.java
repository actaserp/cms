package mes.domain.entity;

import java.sql.Date;
import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Entity
@Table(name="suju")
@NoArgsConstructor
@Data
@EqualsAndHashCode( callSuper=false)
public class Suju extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	Integer id;
	
	@Column(name="\"SujuHead_id\"")
	Integer sujuHeadId;
	
	@Column(name="\"JumunNumber\"")
	String jumunNumber;
	
	@Column(name="\"Material_id\"")
	Integer materialId;
	
	@Column(name="\"SujuQty\"")
	Integer sujuQty;
	
	@Column(name="\"JumunDate\"")
	Date jumunDate;
	
	@Column(name="\"DueDate\"")
	Date dueDate;
	
	@Column(name="\"Company_id\"")
	Integer companyId;
	
	@Column(name="\"CompanyName\"")
	String companyName;
	
	@Column(name="\"ProductionPlanDate\"")
	Timestamp productionPlanDate;
	
	@Column(name="\"ShipmentPlanDate\"")
	Timestamp shipmentPlanDate;
	
	@Column(name="\"Description\"")
	String description;
	
	@Column(name="\"AvailableStock\"")
	Float availableStock;
	
	@Column(name="\"ReservationStock\"")
	Integer reservationStock;
	
	@Column(name="\"SujuQty2\"")
	Integer sujuQty2;
	
	@Column(name="\"UnitPrice\"")
	Double unitPrice;
	
	@Column(name="\"Price\"")
	Double price;
	
	@Column(name="\"Vat\"")
	Double vat;
	
	@Column(name="\"PlanDataPk\"")
	Integer planDataPk;
	
	@Column(name="\"PlanTableName\"")
	String planTableName;
	
	@Column(name="\"State\"")
	String state;
	
	@Column(name="\"ShipmentState\"")
	String shipmentState;
	
	@Column(name="\"SujuType\"")
	String sujuType;
	
	@Column(name="\"_status\"")
	String _status;

	@Column(name="\"InVatYN\"")
	String inVatYN;

	@Column(name="\"TotalAmount\"")
	Double totalAmount;

	@Column(name="\"project_id\"")
	String project_id;

	String spjangcd;

	@Column(name="confirm")
	String confirm;

}
