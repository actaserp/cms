package mes.domain.entity;

import java.sql.Date;
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
@Table(name="action_data")
@NoArgsConstructor
@Data
@EqualsAndHashCode( callSuper=false)
public class ActionData extends AbstractAuditModel {
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"DataPk\"")
	Integer dataPk;
	
	@Column(name = "\"TableName\"")
	String tableName;
	
	@Column(name = "\"Code\"")
	String code;
	
	@Column(name = "\"Type\"")
	String type;
	
	@Column(name = "\"ActorPk\"")
	Integer actorPk;
	
	@Column(name = "\"ActionDateTime\"")
	Timestamp actionDateTime;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"ActorTableName\"")
	String actorTableName;
	
	@Column(name = "\"ActorName\"")
	String actorName;
	
	@Column(name = "\"Char1\"")
	String char1;
	
	@Column(name = "\"Char2\"")
	String char2;
	
	@Column(name = "\"Char3\"")
	String char3;
	
	@Column(name = "\"StartDate\"")
	Date startDate;
	
	@Column(name = "\"EndDate\"")
	Date endDate;
	
	@Column(name = "\"DataPk2\"")
	Integer dataPk2;
	
	@Column(name = "\"TableName2\"")
	String tableName2;
}