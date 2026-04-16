package mes.domain.security;

import java.io.IOException;
import java.io.Serializable;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;



public class CommenceEntryPoint implements AuthenticationEntryPoint, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 766859055713736461L;

	@Override
	public void commence(HttpServletRequest request, HttpServletResponse response,
			AuthenticationException authException) throws IOException, ServletException {

		if ("XMLHttpRequest".equals(request.getHeader("X-Requested-With"))) {
			response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Unauthorized");
		}else {
			response.sendRedirect("/login");
		}

	}

}
