package mes.domain.entity;

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
@Table(name="grid_col")
@Setter
@Getter
@NoArgsConstructor
public class GridColumn extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;

	@Column(name = "\"ModuleName\"")
	String moduleName;

	@Column(name = "\"TemplateKey\"")
	String templateKey;

	@Column(name = "\"GridName\"")
	String gridName;

	@Column(name = "\"Key\"")
	String key;

	@Column(name = "\"Index\"")
	Integer index;

	@Column(name = "\"Label\"")
	String label;

	@Column(name = "\"Width\"")
	Integer width;

	@Column(name = "\"Hidden\"")
	String hidden;
}
