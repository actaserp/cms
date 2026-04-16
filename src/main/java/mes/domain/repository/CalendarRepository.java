package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.Calendar;

@Repository
public interface CalendarRepository extends JpaRepository<Calendar, Integer>{

	Calendar getCalendarById(Integer id);
	

}
