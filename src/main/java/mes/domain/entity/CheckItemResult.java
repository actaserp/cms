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
@Table(name="check_item_result")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class CheckItemResult extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	Integer id;

	@Column(name = "\"CheckResult_id\"")
	Integer checkResultId;

	@Column(name = "\"CheckItem_id\"")
	Integer checkItemId;

	@Column(name = "\"Result1\"")
	String result1;

	@Column(name = "\"Result2\"")
	String result2;

	@Column(name = "\"Result3\"")
	String result3;

	@Column(name = "\"_order\"")
	Integer order;
}
