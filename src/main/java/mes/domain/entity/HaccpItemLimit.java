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
@Table(name="haccp_item_limit")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class HaccpItemLimit extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;

	@Column(name = "\"LowSpec\"")
	Float lowSpec;

	@Column(name = "\"UpperSpec\"")
	Float upperSpec;

	@Column(name = "\"SpecText\"")
	String specText;

	@Column(name = "\"HaccpItem_id\"")
	Integer haccpItemId;

	@Column(name = "\"HaccpProcess_id\"")
	Integer haccpProcessId;

	@Column(name = "\"Material_id\"")
	Integer materialId;
}
