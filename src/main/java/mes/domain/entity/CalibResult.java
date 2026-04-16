package mes.domain.entity;

import java.sql.Date;

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
@Table(name="calib_result")
@Setter
@Getter
@NoArgsConstructor
public class CalibResult extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;

	@Column(name = "\"CalibInstrument_id\"")
	Integer calibInstrumentId;

	@Column(name = "\"CalibDate\"")
	Date calibDate;
	
	@Column(name = "\"CalibInstitution\"")
	String calibInstitution;
	
	@Column(name = "\"Difference\"")
	String difference;
	
	@Column(name = "\"CalibJudge\"")
	String calibJudge;
	
	@Column(name = "\"Description\"")
	String description;
}
