package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.WorkCalendar;

@Repository
public interface WorkCalendarRepository extends JpaRepository<WorkCalendar, Integer>{

	WorkCalendar getWorkCalendarById(Integer id);

}
