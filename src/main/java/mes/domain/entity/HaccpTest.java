package mes.domain.entity;

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
@Table(name="haccp_test")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class HaccpTest extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;

	@Column(name = "\"DataType\"")
	String dataType;

	@Column(name = "\"MaterialName\"")
	String materialName;

	@Column(name = "\"StartTime\"")
	Time startTime;
	
	@Column(name = "\"EndTime\"")
	Time endTime;
	
	@Column(name = "\"Judge\"")
	String judge;
	
	@Column(name = "\"TesterName\"")
	String testerName;
	
	@Column(name = "\"Description\"")
	String description;
	
	
	@Column(name = "\"HaccpDiary_id\"")
	int haccpDiaryId;
	
	@Column(name = "\"Material_id\"")
	int materialId;
	

	@Column(name = "\"Equipment_id\"")
	int equipmentId;

}
