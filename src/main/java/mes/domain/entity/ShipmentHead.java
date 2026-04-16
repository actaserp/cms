package mes.domain.entity;

import java.sql.Time;
import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name="shipment_head")
@Setter
@Getter
@NoArgsConstructor
public class ShipmentHead extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"ShipDate\"")
	Timestamp shipDate;
	
	@Column(name = "\"TotalQty\"")
	Float totalQty;
	
	@Column(name = "\"TotalPrice\"")
	Double totalPrice;
	
	@Column(name = "\"TotalVat\"")
	Double totalVat;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"State\"")
	String state;
	
	@Column(name = "\"StatementIssuedYN\"")
	String statementIssuedYN;
	
	@Column(name = "\"StatementNumber\"")
	String statementNumber;
	
	@Column(name = "\"IssueDate\"")
	Timestamp issueDate;
	
	@Column(name = "\"CarType\"")
	String carType;
	
	@Column(name = "\"LoadOrder\"")
	Integer loadOrder;
	
	@Column(name = "\"Company_id\"")
	Integer companyId;
	
	@Column(name = "\"OrderDate\"")
	Timestamp orderDate;
	
	@Column(name = "\"ShipTime\"")
	Time shipTime;

	@Column(name = "\"misnum\"")
	Integer misnum; // 세금계산서 발행 pk
	
}
