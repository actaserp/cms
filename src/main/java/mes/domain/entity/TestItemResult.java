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
@Table(name="test_item_result")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class TestItemResult extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"SampleID\"")
	String sampleID;
	
	@Column(name = "\"InputResult\"")
	String inputResult;
	
	@Column(name = "\"NumResult\"")
	Float numResult;
	
	@Column(name = "\"CharResult\"")
	String charResult;
	
	@Column(name = "\"SpecType\"")
	String specType;
	
	@Column(name = "\"LowSpec\"")
	Float lowSpec;
	
	@Column(name = "\"UpperSpec\"")
	Float upperSpec;
	
	@Column(name = "\"SpecText\"")
	String specText;
	
	@Column(name = "\"Off\"")
	Integer off;
	
	@Column(name = "\"_order\"")
	Integer order;
	
	@Column(name = "\"TestItem_id\"")
	Integer testItemId;
	
	@Column(name = "\"TestResult_id\"")
	Integer testResultId;
	
	@Column(name = "\"TestDateTime\"")
	Timestamp testDateTime;
	
	@Column(name = "\"JudgeCode\"")
	String judgeCode;
	
	@Column(name = "\"Char1\"")
	String char1;
	
	@Column(name = "\"Char2\"")
	String char2;
	
	@Column(name = "\"SourceTableName\"")
	String sourceTableName;
	
	@Column(name = "\"SourceDataPk\"")
	Integer sourceDataPk;

	String spjangcd;
}
