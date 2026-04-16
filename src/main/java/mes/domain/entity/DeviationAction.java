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
@Table(name="devi_action")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class DeviationAction extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;

	@Column(name = "\"SourceDataPk\"")
	Integer sourceDataPk;

	@Column(name = "\"SourceTableName\"")
	String sourceTableName;

	@Column(name = "\"HappenDate\"")
	Date happenDate;

	@Column(name = "\"HappenPlace\"")
	String happenPlace;

	@Column(name = "\"AbnormalDetail\"")
	String abnormalDetail;

	@Column(name = "\"ActionDetail\"")
	String actionDetail;

	@Column(name = "\"ConfirmDetail\"")
	String confirmDetail;
	
	@Column(name = "\"ActionState\"")
	String actionState;

	@Column(name = "\"ConfirmState\"")
	String confirmState;
}
