package mes.domain.entity;

import java.sql.Date;
import java.sql.Time;

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
@Table(name="master_result")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class MasterResult extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;

	@Column(name = "\"MasterClass\"")
	String masterClass;

	@Column(name = "\"MasterTable_id\"")
	Integer masterTableId;

	@Column(name = "\"DataDate\"")
	Date dataDate;

	@Column(name = "\"DataTime\"")
	Time dataTime;

	@Column(name = "\"Number1\"")
	Integer number1;

	@Column(name = "\"Number2\"")
	Integer number2;

	@Column(name = "\"Number3\"")
	Integer number3;

	@Column(name = "\"Number4\"")
	Integer number4;

	@Column(name = "\"Char1\"")
	String char1;

	@Column(name = "\"Char2\"")
	String char2;

	@Column(name = "\"Char3\"")
	String char3;

	@Column(name = "\"Char4\"")
	String char4;

	@Column(name = "\"Char5\"")
	String char5;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"SourceDataPk\"")
	Integer sourceDataPk;

	@Column(name = "\"SourceTableName\"")
	String sourceTableName;
}
