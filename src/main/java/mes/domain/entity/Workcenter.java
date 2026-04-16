package mes.domain.entity;

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
@Table(name="work_center")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class Workcenter extends AbstractAuditModel {
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"Code\"")
	String code;
	
	@Column(name = "\"Name\"")
	String name;
	
	@Column(name = "\"HierarchyLevel\"")
	String hierarchyLevel;
	
	@Column(name = "\"WorkerCount\"")
	Integer workerCount;
	
	@Column(name = "\"OutSourcingYN\"")
	String outSourcingYN;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"Area_id\"")
	Integer areaId;
	
	@Column(name = "\"Factory_id\"")
	Integer factoryId;
	
	@Column(name = "\"Process_id\"")
	Integer processId;
	
	@Column(name = "\"ProcessStoreHouse_id\"")
	Integer proccesStoreHouseId;

	@Column(name = "spjangcd")
	String spjangcd;

}
