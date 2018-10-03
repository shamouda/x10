package x10.core.security;

import java.security.MessageDigest;
import x10.core.Rail;

@SuppressWarnings("serial")
public class NativeSHA  {
    private MessageDigest md;
    
    public NativeSHA(System[] $dummy) {
        
    }
    
    public final NativeSHA x10$core$security$NativeSHA$$init$S() {
        try {
            md = MessageDigest.getInstance("SHA-1");
        }
        catch(Exception ex) {
            
        }
        return this;
    }

    public void digest__0$1x10$lang$Byte$2(Rail hash, int offset, int len) {
        try{
            md.digest(hash.getByteArray(), offset, len);
        }catch(Exception ex) {
            throw new x10.util.security.SHAException(ex.getMessage());
        }
    }
    
    public void update__0$1x10$lang$Byte$2(Rail hash, int offset, int len) {
        try{
            md.update(hash.getByteArray(), offset, len);
        }catch(Exception ex) {
            throw new x10.util.security.SHAException(ex.getMessage());
        }
    }
}
