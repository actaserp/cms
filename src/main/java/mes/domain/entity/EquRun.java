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
@Table(name="equ_run")
@Setter
@Getter
@NoArgsConstructor
public class EquRun extends AbstractAuditModel{
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"StartDate\"")
	Timestamp startDate;
	
	@Column(name = "\"EndDate\"")
	Timestamp endDate;
	
	@Column(name = "\"RunState\"")
	String runState;
	
	@Column(name = "\"WorkOrderNumber\"")
	String workOrderNumber;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"Type\"")
	String type;
	
	@Column(name = "\"Equipment_id\"")
	Integer equipmentId;
	
	@Column(name = "\"StopCause_id\"")
	Integer stopCauseId;

	String spjangcd;
}
