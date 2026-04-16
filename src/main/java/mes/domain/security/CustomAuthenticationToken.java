package mes.domain.security;

import java.util.Collection;

import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;

import mes.domain.entity.User;

public class CustomAuthenticationToken extends AbstractAuthenticationToken{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private User principal;
	
	private String credential;
	
	public CustomAuthenticationToken(User principal, String credential, Collection<? extends GrantedAuthority> authorities) {
		super(authorities);
		
		//User 객체로 해야함
		this.setDetails(principal);
	    this.principal = principal;
	    this.credential = credential;
	    this.setAuthenticated(true);
	}

	@Override
	public Object getCredentials() {
		return this.credential;
	}

	@Override
	public Object getPrincipal() {
		// TODO Auto-generated method stub
		return this.principal;
	}

}
