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
@Table(name="haccp_item_result")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class HaccpItemResult extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"HaccpTest_id\"")
	int haccpTestId;
	
	@Column(name = "\"HaccpItem_id\"")
	int haccpItemId;

	@Column(name = "\"DataDiv\"")
	String dataDiv;

	@Column(name = "\"NumResult\"")
	Float numResult;
	
	@Column(name = "\"CharResult\"")
	String charResult;
	
	@Column(name = "\"LowSpec\"")
	Float lowSpec;
	
	@Column(name = "\"UpperSpec\"")
	Float upperSpec;
	
	@Column(name = "\"Off\"")
	Integer off;	
	
}
