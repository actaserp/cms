package mes.app.schedule.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class MatOrderService {

	@Autowired
	SqlRunner sqlRunner;
}
