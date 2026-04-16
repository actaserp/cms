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
@Table(name="test_result")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class TestResult extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	Integer id;
	
	@Column(name = "\"TestDateTime\"")
	Timestamp testDateTime;
	
	@Column(name = "\"LotNumber\"")
	String lotNumber;
	
	@Column(name = "\"JudgeCode\"")
	String judgeCode;
	
	@Column(name = "\"TestCount\"")
	Integer testCount;
	
	@Column(name = "\"PassCount\"")
	Integer passCount;
	
	@Column(name = "\"RejectCount\"")
	Integer rejectCount;
	
	@Column(name = "\"Tester_id\"")
	Integer testerId;
	
	@Column(name = "\"TestRemark\"")
	String testRemark;
	
	@Column(name = "\"State\"")
	String state;
	
	@Column(name = "\"SourceDataPk\"")
	Integer sourceDataPk;
	
	@Column(name = "\"SourceTableName\"")
	String sourceTableName;
	
	@Column(name = "\"SampleID\"")
	String sampleID;
	
	@Column(name = "\"Material_id\"")
	Integer materialId;
	
	@Column(name = "\"TestMaster_id\"")
	Integer testMasterId;

	String spjangcd;
}
