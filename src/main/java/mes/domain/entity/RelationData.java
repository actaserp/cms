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
@Table(name="rela_data")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class RelationData extends AbstractAuditModel {
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"DataPk1\"")
	Integer dataPk1;
	
	@Column(name = "\"TableName1\"")
	String tableName1;
	
	@Column(name = "\"DataPk2\"")
	Integer dataPk2;
	
	@Column(name = "\"TableName2\"")
	String tableName2;
	
	@Column(name = "\"RelationName\"")
	String relationName;
	
	@Column(name = "\"Char1\"")
	String char1;
	
	@Column(name = "\"Number1\"")
	Integer number1;
	
	@Column(name = "\"Date1\"")
	Timestamp Date1;
	
	@Column(name = "\"Text1\"")
	String text1;

	@Column(name = "\"StartDate\"")
	Timestamp StartDate;
	
	@Column(name = "\"EndDate\"")
	Timestamp EndDate;
	
	@Column(name = "\"_order\"")
	Integer _order;
}
