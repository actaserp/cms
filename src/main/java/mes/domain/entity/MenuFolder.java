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
@Table(name="menu_folder")
@Setter
@Getter
@NoArgsConstructor
public class MenuFolder extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;

	@Column(name = "\"FolderName\"")
	String folderName;

	@Column(name = "\"IconCSS\"")
	String iconCss;

	@Column(name = "\"_order\"")
	Integer order;

	@Column(name = "\"Parent_id\"")
	Integer parentId;
}
