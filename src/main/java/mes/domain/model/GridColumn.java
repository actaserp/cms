package mes.domain.model;


import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import mes.domain.entity.AbstractAuditModel;

@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class GridColumn extends AbstractAuditModel {	
	Integer id;
	String moduleName;
	String templateKey;
	String gridName;
	String key;
	Integer index;
	String label;
	Integer width;
	String hidden;	
}
