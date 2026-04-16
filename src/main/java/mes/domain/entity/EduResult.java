package mes.domain.entity;


import java.util.Date;

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
@Table(name="edu_result")
@Setter
@Getter
@NoArgsConstructor
public class EduResult extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"EduDate\"")
	Date eduDate;
	
	@Column(name = "\"EduTitle\"")
	String eduTitle;
	
	@Column(name = "\"EduPlace\"")
	String eduPlace;
	
	@Column(name = "\"Teacher\"")
	String teacher;
	
	@Column(name = "\"EduHour\"")
	Integer eduHour;
	
	@Column(name = "\"StartTime\"")
	String startTime;
	
	@Column(name = "\"EndTime\"")
	String endTime;
	
	@Column(name = "\"EduTarget\"")
	String eduTarget;
	
	@Column(name = "\"TargetCount\"")
	Integer targetCount;
	
	@Column(name = "\"StudentCount\"")
	Integer studentCount;
	
	@Column(name = "\"EduContent\"")
	String eduContent;
	
	@Column(name = "\"EduMaterial\"")
	String eduMaterial;
	
	@Column(name = "\"AbsenteeProcess\"")
	String absenteeProcess;
	
	@Column(name = "\"EduEvaluation\"")
	String eduEvaluation;
	
	@Column(name = "\"SourceDataPk\"")
	Integer sourceDataPk;
	
	@Column(name = "\"SourceTableName\"")
	String sourceTableName;
	
	@Column(name = "\"_status\"")
	String _status;
	
}
