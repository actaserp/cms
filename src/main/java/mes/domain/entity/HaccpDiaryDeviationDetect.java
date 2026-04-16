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
@Table(name="haccp_diary_devi_detect")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class HaccpDiaryDeviationDetect extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"HaccpDiary_id\"")
	Integer haccpDiaryId;
	
	@Column(name = "\"HaccpTest_id\"")
	Integer haccpTestId;
	
	@Column(name = "\"HaccpItem_id\"")
	Integer haccpItemId;
	
	@Column(name = "\"HappenPlace\"")
	String happenPlace;

	@Column(name = "\"StartTime\"")
	Time startTime;

	@Column(name = "\"EndTime\"")
	Time endTime;
	
	@Column(name = "\"HappenTime\"")
	Time happenTime;

	@Column(name = "\"AbnormalDetail\"")
	String abnormalDetail;
	
	@Column(name = "\"ActionDetail\"")
	String actionDetail;
	
	@Column(name = "\"ActionCode\"")
	String actionCode;
	
	
	@Column(name = "\"ActorName\"")
	String actorName;	
	
	@Column(name = "\"Substance\"")
	String substance;	

	@Column(name = "\"Quantity\"")
	Float quantity;	
	
	@Column(name = "\"Material_id\"")
	Integer materialId;

	@Column(name = "\"Description\"")
	String description;
}
