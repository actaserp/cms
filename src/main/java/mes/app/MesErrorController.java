package mes.app;

import java.io.IOException;
import javax.servlet.RequestDispatcher;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import mes.domain.services.LogWriter;

@Controller
public class MesErrorController implements ErrorController  {
	
	@Autowired
	LogWriter logWriter;	
	
	@RequestMapping("/error")
    public String pageError(HttpServletRequest request, HttpServletResponse response, HttpSession session) {
		Object status = request.getAttribute(RequestDispatcher.ERROR_STATUS_CODE);
		if(status==null) {
			return "errors/500";
		}
		Integer statusCode = Integer.valueOf(status.toString());

		boolean isAjax = this.isAjax(request);
		String viewName = String.format("errors/%d", statusCode);

		if(isAjax) {
			try {
				if(statusCode==401) {
					response.sendError(401, "Unauthorized");
				}else if(statusCode==403) {
					response.sendError(403, "Forbidden");
				}
				else if(statusCode==404) {
					response.sendError(404, "Not Found");
				}else {
					response.sendError(500, "Internal Server Error");
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return viewName;
	}
	
	private boolean isAjax(HttpServletRequest request) {
	    String requestedWithHeader = request.getHeader("X-Requested-With");
	    return "XMLHttpRequest".equals(requestedWithHeader);
	}
}