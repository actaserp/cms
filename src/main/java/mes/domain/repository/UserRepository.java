package mes.domain.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import mes.domain.entity.User;

import javax.transaction.Transactional;

@Repository
public interface UserRepository extends JpaRepository<User, Integer> {

	Optional<User> findByUsername(String username);
	User getUserById(int id);

	@Query(value = "SELECT COUNT(*) > 0 FROM auth_user WHERE username = :usernm AND email = :mail", nativeQuery = true)
	boolean existsByUsernameAndEmail(@Param("usernm") String usernm, @Param("mail") String mail);


	@Query(value = "SELECT username FROM auth_user WHERE first_name = :firstName AND email = :email", nativeQuery = true)
	List<String> findByFirstNameAndEmailNative(@Param("firstName") String firstName, @Param("email") String email);

	@Transactional
	@Modifying
	@Query(value = "update auth_user set password = :pw WHERE username = :userid", nativeQuery = true)
	void PasswordChange(@Param("pw") String pw, @Param("userid") String userid);

	Optional<User> findBySpjangcd(String spjangcd);

}
