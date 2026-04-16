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
@Table(name="menu_use_log")
@Setter
@Getter
@NoArgsConstructor
public class MenuUseLog extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;

	@Column(name = "\"MenuCode\"")
	String menuCode;

	@Column(name = "\"User_id\"")
	Integer userId;

	@Column(name = "\"spjangcd\"")
	String spjangcd;
}
