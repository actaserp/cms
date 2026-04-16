package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import mes.domain.entity.TagGroup;

public interface TagGroupRepository extends JpaRepository<TagGroup, Integer> {

	
	TagGroup getTagGroupById(Integer id);

	
}
