package mes.domain.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name="tag")
@Setter
@Getter
@NoArgsConstructor
public class Tag extends AbstractAuditModel{

	@Id
	@Column(name = "\"tag_code\"")
	String tagCode;
	
	
	@Column(name = "\"tag_name\"")
	String tag_name;
	
	@Column(name = "\"tag_group_id\"")
	Integer tag_group_id;
	
	@Column(name = "\"Equipment_id\"")
	Integer equipment_id;
	
	@Column(name = "\"RoundDigit\"")
	Integer round_digit;
	
	@Column(name = "\"DASConfig_id\"")
	Integer DASConfig_id;
	
	@Column(name = "\"LSL\"")
	Float LSL;
	
	@Column(name = "\"USL\"")
	Float USL;

	
	
	
}
