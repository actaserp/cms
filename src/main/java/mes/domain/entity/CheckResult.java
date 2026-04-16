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
@Table(name="check_result")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class CheckResult extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	Integer id;

	@Column(name = "\"CheckMaster_id\"")
	Integer checkMasterId;
	
	@Column(name = "\"CheckDate\"")
	Date checkDate;
	
	@Column(name = "\"CheckTime\"")
	String checkTime;
	
	@Column(name = "\"TargetName\"")
	String targetName;
	
	@Column(name = "\"SourceDataPk\"")
	Integer sourceDataPk;
	
	@Column(name = "\"SourceTableName\"")
	String sourceTableName;
	
	@Column(name = "\"Checker_id\"")
	Integer checkerId;
	
	@Column(name = "\"CheckerName\"")
	String checkerName;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"Char1\"")
	String char1;
	
	@Column(name = "\"Char2\"")
	String char2;
	
	@Column(name = "\"Number1\"")
	Integer number1;
	
	@Column(name = "\"Number2\"")
	Integer number2;
	
}
