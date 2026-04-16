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
@Table(name="task_master")
@Setter
@Getter
@NoArgsConstructor
public class TaskMaster extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"Code\"")
	String code;
	
	@Column(name = "\"GroupCode\"")
	String groupCode;
	
	@Column(name = "\"TaskName\"")
	String taskName;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"Line1Name\"")
	String line1Name;
	
	@Column(name = "\"Approver1_id\"")
	Integer approver1Id;
	
	@Column(name = "\"Line2Name\"")
	String line2Name;
	
	@Column(name = "\"Approver2_id\"")
	Integer approver2Id;
	
	@Column(name = "\"CycleBase\"")
	String cycleBase;
	
	@Column(name = "\"CycleNumber\"")
	Float cycleNumber;
	
	@Column(name = "\"NotificationYN\"")
	String notificationYN;
	
	@Column(name = "\"NotificationBefore\"")
	Float notificationBefore;
	
	@Column(name = "\"NotificationPlanDate\"")
	Timestamp notificationPlanDate;
	
	@Column(name = "\"LastWriteDate\"")
	Timestamp lastWriteDate;
	
	@Column(name = "\"NextWriteDate\"")
	Timestamp nextWriteDate;
	
	@Column(name = "\"Line3Name\"")
	String line3Name;
	
	@Column(name = "\"Approver3_id\"")
	Integer approver3Id;
	
	@Column(name = "\"Line4Name\"")
	String line4Name;
	
	@Column(name = "\"Approver4_id\"")
	Integer approver4Id;
	
	@Column(name = "\"WriterGroup_id\"")
	Integer writerGroupId;
}
