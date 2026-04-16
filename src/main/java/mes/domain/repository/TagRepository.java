package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import mes.domain.entity.Tag;

public interface TagRepository extends JpaRepository<Tag, Integer>{
	
	Tag getByTagCode(String tag_code);


}
