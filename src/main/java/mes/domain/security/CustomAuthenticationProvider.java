package mes.domain.security;

import java.util.ArrayList;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.stereotype.Component;

import mes.domain.entity.User;
import mes.domain.entity.UserGroup;
import mes.domain.repository.UserRepository;

@Component
public class CustomAuthenticationProvider  implements AuthenticationProvider  {

	@Autowired
	UserRepository userRepository;
	
	@Override
	public Authentication authenticate(Authentication authentication) throws AuthenticationException {
		
		 String username = authentication.getName();
	     String password = authentication.getCredentials().toString();
	     
	     Optional<User> optUser = this.userRepository.findByUsername(username);
	     if(optUser.isPresent()) {
	    	 User user = optUser.get();
	    	 boolean valid = Pbkdf2Sha256.verification(password, user.getPassword());
	    	 if (valid) {
	    		 // 그룹을  array 에 추가한다
	    		 UserGroup group = user.getUserProfile().getUserGroup();
	    		 SimpleGrantedAuthority authority = new SimpleGrantedAuthority(group.getCode());
	    		 ArrayList<SimpleGrantedAuthority> authorities =  new ArrayList<SimpleGrantedAuthority>();
	    		 authorities.add(authority);	    		 
	    		 
	    		 return new CustomAuthenticationToken(user, password, authorities);	 
	    	 }	    	 
	     }else {
	    	 //throw new UsernameNotFoundException(/*principal name*/);
	     }

		return null;
	}

	@Override
	public boolean supports(Class<?> authentication) {
		// TODO Auto-generated method stub
		return authentication.equals(UsernamePasswordAuthenticationToken.class);
	}


}
