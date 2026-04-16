package mes.domain.entity;

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
@Table(name="mat_order")
@Setter
@Getter
@NoArgsConstructor
public class MatOrder extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"OrderNumber\"")
	String orderNumber;
	
	@Column(name = "\"LineNum\"")
	Integer lineNum;
	
	@Column(name = "\"OrderDate\"")
	Timestamp orderDate;
	
	@Column(name = "\"AvailableStock\"")
	Float availableStock;
	
	@Column(name = "\"SafetyStock\"")
	Float safetyStock;
	
	@Column(name = "\"OrderQty\"")
	Float orderQty;
	
	@Column(name = "\"PackOrderQty\"")
	Float packOrderQty;
	
	@Column(name = "\"UnitPrice\"")
	Float unitPrice;
	
	@Column(name = "\"TotalPrice\"")
	Float totalPrice;
	
	@Column(name = "\"InputPlanDate\"")
	Timestamp inputPlanDate;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"LotNo\"")
	String lotNo;
	
	@Column(name = "\"State\"")
	String state;
	
	@Column(name = "\"ChargerCode\"")
	String chargerCode;
	
	@Column(name = "\"Actor_id\"")
	Integer actorId;
	
	@Column(name = "\"Approver_id\"")
	Integer approverId;
	
	@Column(name = "\"ApproveDateTime\"")
	Timestamp approveDateTime;
	
	@Column(name = "\"Company_id\"")
	Integer companyId;
	
	@Column(name = "\"Material_id\"")
	Integer materialId;
	
	@Column(name = "\"MaterialRequirement_id\"")
	Integer materialRequirementId;
}
