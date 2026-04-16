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
@Table(name="grid_col_lang")
@Setter
@Getter
@NoArgsConstructor
public class GridColLang extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"LangCode\"")
	String langCode;
	
	@Column(name = "\"DispText\"")
	String dispText;
	
	@Column(name = "\"GridColumn_id\"")
	Integer gridColumnId;
}
