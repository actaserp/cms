package mes.domain.entity;

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
@Table(name="task_approver")
@Setter
@Getter
@NoArgsConstructor
public class TaskApprover extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"Line\"")
	Integer line;
	
	@Column(name = "\"Shift\"")
	String shift;
	
	@Column(name = "\"User_id\"")
	Integer userId;
	
	@Column(name = "\"Depart_id\"")
	Integer departId;
	
	@Column(name = "\"TaskMaster_id\"")
	Integer taskMasterId;
	
}
