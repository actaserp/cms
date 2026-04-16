package mes.domain.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name="menu_item")
@Setter
@Getter
@NoArgsConstructor
public class MenuItem extends AbstractAuditModel {
	
	@Id
	@Column(name = "\"MenuCode\"")
	String menuCode;

	@Column(name = "\"MenuName\"")
	String menuName;

	@Column(name = "\"IconCSS\"")
	String iconCSS;

	@Column(name = "\"Url\"")
	String url;

	@Column(name = "\"_order\"")
	Integer order;

	@Column(name = "\"MenuFolder_id\"")
	Integer menuFolderId;
}
