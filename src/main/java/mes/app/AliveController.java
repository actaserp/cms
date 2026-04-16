package mes.app;

import javax.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import mes.domain.model.Alive;


@RestController
public class AliveController {
	@RequestMapping(value= "/alive", method=RequestMethod.GET)
    public Alive alive(HttpServletRequest request) {
		Alive alive = new Alive();
		return alive; 
	}
}
