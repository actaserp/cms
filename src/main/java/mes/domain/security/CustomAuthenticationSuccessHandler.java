package mes.domain.security;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.AuthenticationSuccessHandler;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import mes.domain.model.AjaxResult;
import mes.domain.services.AccountService;

@Component
public class CustomAuthenticationSuccessHandler implements AuthenticationSuccessHandler {

	@Autowired
	AccountService accountService;
		
	@Override
	public void onAuthenticationSuccess(HttpServletRequest request, HttpServletResponse response,
			Authentication authentication) throws IOException, ServletException {

		ObjectMapper mapper = new ObjectMapper();
		AjaxResult result = new AjaxResult();
		result.success = true;
		result.message="OK";
		result.data = "OK";
		
		response.setCharacterEncoding("UTF-8");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().print(mapper.writeValueAsString(result));
        response.getWriter().flush();		
        
		this.accountService.saveLoginLog("login", authentication, request);
	}
}
