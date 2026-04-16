package mes.domain.entity;

import java.sql.Timestamp;
import java.util.Date;

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
@Table(name="job_res")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class JobRes extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"WorkOrderNumber\"")
	String workOrderNumber;
	
	@Column(name = "\"ProductionPlanDate\"")
	Timestamp productionPlanDate;
	
	@Column(name = "\"ProductionDate\"")
	Timestamp productionDate;
	
	@Column(name = "\"ShiftCode\"")
	String shiftCode;
	
	@Column(name = "\"WorkIndex\"")
	Integer workIndex;
	
	@Column(name = "\"OrderQty\"")
	Float orderQty;
	
	@Column(name = "\"State\"")
	String state;
	
	@Column(name = "\"GoodQty\"")
	Float goodQty;
	
	@Column(name = "\"DefectQty\"")
	Float defectQty;
	
	@Column(name = "\"ReworkQty\"")
	Float reworkQty;
	
	@Column(name = "\"LossQty\"")
	Float lossQty;
	
	@Column(name = "\"ScrapQty\"")
	Float scrapQty;
	
	@Column(name = "\"StartTime\"")
	Timestamp startTime;
	
	@Column(name = "\"EndTime\"")
	Timestamp endTime;
	
	@Column(name = "\"WorkerCount\"")
	Integer workerCount;
	
	@Column(name = "\"Manager_id\"")
	Integer manager_id;
	
	@Column(name = "\"LotNumber\"")
	String lotNumber;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"Parent_id\"")
	Integer parent_id;
	
	@Column(name = "\"LotCount\"")
	Integer lotCount;
	
	@Column(name = "\"ProcessCount\"")
	Integer processCount;
	
	@Column(name = "\"SourceDataPk\"")
	Integer sourceDataPk;
	
	@Column(name = "\"SourceTableName\"")
	String sourceTableName;
	
	@Column(name = "\"Equipment_id\"")
	Integer equipment_id;
	
	@Column(name = "\"FirstWorkCenter_id\"")
	Integer firstWorkCenter_id;
	
	@Column(name = "\"Material_id\"")
	Integer materialId;
	
	@Column(name = "\"MaterialProcessInputRequest_id\"")
	Integer materialProcessInputRequestId;
	
	@Column(name = "\"Routing_id\"")
	Integer routing_id;
	
	@Column(name = "\"StoreHouse_id\"")
	Integer storeHouse_id;
	
	@Column(name = "\"WorkCenter_id\"")
	Integer workCenter_id;
	
	@Column(name = "\"EndDate\"")
	Date endDate;

	String spjangcd;
}
