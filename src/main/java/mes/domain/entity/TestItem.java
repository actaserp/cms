package mes.domain.entity;

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
@Table(name="test_item")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class TestItem extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"Code\"")
	String code;
	
	@Column(name = "\"Name\"")
	String name;
	
	@Column(name = "\"EngName\"")
	String engName;
	
	@Column(name = "\"ItemType\"")
	String itemType;
	
	@Column(name = "\"ResultType\"")
	String resultType;
	
	@Column(name = "\"RoundDigit\"")
	Integer roundDigit;
	
	@Column(name = "\"TestMethod_id\"")
	Integer testMethodId;
	
	@Column(name = "\"Unit_id\"")
	Integer unitId;
}
