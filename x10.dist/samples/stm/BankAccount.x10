import x10.util.resilient.localstore.Cloneable;
import x10.util.resilient.localstore.LocalStore;

class BankAccount implements Cloneable{
    var account:Long;
    
    public def this(a:Long) {
        account = a;
    }
    
    public def clone():Cloneable {
        return new BankAccount(account) as Cloneable;
    }
    
    public def toString() {
        return "account:" + account;
    }
}