package mes.domain.entity;

import java.sql.Date;

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
@Table(name="check_target")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class CheckTarget extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;

	@Column(name = "\"CheckMaster_id\"")
	Integer checkMasterId;
	//Integer checkMaster_id;

	@Column(name = "\"TargetName\"")
	String targetName;

	@Column(name = "\"SourceDataPk\"")
	Integer sourceDataPk;

	@Column(name = "\"SourceTableName\"")
	String sourceTableName;

	@Column(name = "\"TargetGroup1\"")
	String targetGroup1;

	@Column(name = "\"TargetGroup2\"")
	String targetGroup2;

	@Column(name = "\"StartDate\"")
	Date startDate;

	@Column(name = "\"EndDate\"")
	Date endDate;

	@Column(name = "\"_order\"")
	Integer order;
}
