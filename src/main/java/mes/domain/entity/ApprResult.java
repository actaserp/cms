package mes.domain.entity;

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
@Table(name="appr_result")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class ApprResult extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	Integer id;
	
	@Column(name = "\"SourceDataPk\"")
	Integer sourceDataPk;
	
	@Column(name = "\"SourceTableName\"")
	String sourceTableName;
	
	@Column(name = "\"Line\"")
	Integer line;
	
	@Column(name = "\"LineName\"")
	String lineName;
	
	@Column(name = "\"ApprDate\"")
	Timestamp apprDate;
	
	@Column(name = "\"Approver_id\"")
	Integer approverId;
	
	@Column(name = "\"State\"")
	String state;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"OriginTableName\"")
	String originTableName;
	
	@Column(name = "\"ApprStepYN\"")
	String apprStepYN;
	
	@Column(name = "\"OriginGui\"")
	String originGui;
	
	@Column(name = "\"OriginGuiParam\"")
	String originGuiParam;
}
