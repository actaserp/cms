package mes.domain.repository;

import java.util.List;
import java.util.Optional;

import mes.domain.entity.User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.UserGroup;

@Repository
public interface UserGroupRepository extends JpaRepository<UserGroup, Integer> {

	List<UserGroup> findBySpjangcdAndName(String spjangcd, String name);
	
	UserGroup getUserGrpById(Integer id);

	List<UserGroup> findBySpjangcd(String spjangcd);

}
