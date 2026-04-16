package mes.domain.entity;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;

@Entity
@Table(name="menu_front_folder")
@Setter
@Getter
@NoArgsConstructor
public class MenuFrontFolder extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	Integer id;

	@Column(name = "\"folder_name\"")
	String folder_name;

	@Column(name = "\"icon_css\"")
	String icon_css;

	@Column(name = "\"dom_id\"")
	String dom_id;

	@Column(name = "\"_order\"")
	Integer _order;

	@Column(name = "\"use_yn\"")
	String use_yn;
}
