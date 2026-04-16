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
@Table(name="equip_history")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class EquipmentHistory extends AbstractAuditModel {
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;

	@Column(name = "\"Equipment_id\"")
	Integer equipmentId;

	@Column(name = "\"DataDate\"")
	Date dataDate;

	@Column(name = "\"Description\"")
	String description;

	@Column(name = "\"Content\"")
	String content;

	@Column(name = "\"Cost\"")
	Integer cost;
	
	@Column(name = "\"Char1\"")
	String char1;
	
	@Column(name = "\"Char2\"")
	String char2;
	
	@Column(name = "\"SourceDataPk\"")
	Integer sourceDataPk;
	
	@Column(name = "\"SourceTableName\"")
	String sourceTableName;
	
	@Column(name = "\"Text1\"")
	String text1;
	
	@Column(name = "\"ApprDataPk\"")
	Integer apprDataPk;
	
	@Column(name = "\"ApprTableName\"")
	String apprTableName;
}
