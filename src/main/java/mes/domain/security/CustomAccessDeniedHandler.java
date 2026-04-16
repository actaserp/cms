package mes.domain.security;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.security.web.access.AccessDeniedHandler;
public class CustomAccessDeniedHandler implements AccessDeniedHandler {
    
	@Override
	public void handle(HttpServletRequest request, HttpServletResponse response,
			org.springframework.security.access.AccessDeniedException accessDeniedException)
			throws IOException, ServletException {
        //Authentication auth = SecurityContextHolder.getContext().getAuthentication();        
        
        String requestedWithHeader = request.getHeader("X-Requested-With");
        
        // ajax 요청이면 status 만 리턴
        if("XMLHttpRequest".equals(requestedWithHeader)) {
        	response.sendError(HttpServletResponse.SC_FORBIDDEN, "FORBIDDEN");        	
        	
        }else {
        	response.sendRedirect(request.getContextPath() + "/errors/403");
        }
	}
}