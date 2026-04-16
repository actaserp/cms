package mes.domain.entity;

import java.sql.Timestamp;

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
@Table(name="check_item")
@Setter
@Getter
@NoArgsConstructor
public class CheckItem extends AbstractAuditModel {
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"CheckMaster_id\"")
	Integer checkMasterId;
	
	@Column(name = "\"Code\"")
	String code;
	
	@Column(name = "\"Name\"")
	String name;
	
	@Column(name = "\"ItemGroup1\"")
	String itemGroup1;
	
	@Column(name = "\"ItemGroup2\"")
	String itemGroup2;
	
	@Column(name = "\"ItemGroup3\"")
	String itemGroup3;
	
	@Column(name = "\"CycleValue\"")
	Integer cycleValue;
	
	@Column(name = "\"CycleType\"")
	String cycleType;
	
	@Column(name = "\"ResultType\"")
	String resultType;
	
	@Column(name = "\"StartDate\"")
	Timestamp StartDate;
	
	@Column(name = "\"EndDate\"")
	Timestamp EndDate;
	
	@Column(name = "\"_order\"")
	Integer order;
	
	@Column(name = "\"minValue\"")
	Integer minValue;
	
	@Column(name = "\"maxValue\"")
	Integer maxValue;
}
