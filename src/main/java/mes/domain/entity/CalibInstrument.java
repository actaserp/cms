package mes.domain.entity;

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
@Table(name="calib_inst")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class CalibInstrument extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;

	@Column(name = "\"Name\"")
	String name;

	@Column(name = "\"CalibInstClass\"")
	String calibInstClass;

	@Column(name = "\"CycleBase\"")
	String cycleBase;

	@Column(name = "\"CycleNumber\"")
	Float cycleNumber;

	@Column(name = "\"AuthorizedCalibDate\"")
	Date authorizedCalibDate;

	@Column(name = "\"SelfCalibDate\"")
	Date selfCalibDate;

	@Column(name = "\"NextCalibDate\"")
	Date nextCalibDate;

	@Column(name = "\"CalibJudge\"")
	String calibJudge;

	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"SourceDataPk\"")
	Integer sourceDataPk;
	
	@Column(name = "\"SourceTableName\"")
	String sourceTableName;

	@Column(name = "\"StartDate\"")
	Date startDate;

	@Column(name = "\"EndDate\"")
	Date endDate;
}
