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
@Table(name="storyboard_item")
@Setter
@Getter
@NoArgsConstructor
public class StoryBoardItem extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"MenuCode\"")
	String menuCode;
	
	@Column(name = "\"BoardType\"")
	String boardType;
	
	@Column(name = "\"Duration\"")
	Integer duration;
	
	@Column(name = "\"ParameterData\"")
	String parameterData;
	
	@Column(name = "\"Url\"")
	String url;
}
