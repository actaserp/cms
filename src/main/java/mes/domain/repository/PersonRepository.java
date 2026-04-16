package mes.domain.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.Person;

@Repository
public interface PersonRepository extends JpaRepository<Person, Integer>{

	Person getPersonById(Integer id);
	Optional<Person> findByCode(String code);
	Optional<Person> findByName(String name);

}
