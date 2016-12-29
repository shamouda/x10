import x10.util.resilient.localstore.tx.*;

public class TestLocks {
    public static def main(args:Rail[String]) {
        try {
            testReaders();
            Console.OUT.println("testReaders success");
        } catch(e:Exception) {
            Console.OUT.println("testReaders failed");
        }
        try {
            testWriters();
            Console.OUT.println("testWriters failed");
        } catch(e:Exception) {
            Console.OUT.println("testWriters success");
        }
        
        try {
            testReadersWriter();
            Console.OUT.println("testReadersWriter failed");
        } catch(e:Exception) {
            Console.OUT.println("testReadersWriter success");
        }
        
        try {
            testSameReaderWriter();
            Console.OUT.println("testSameReaderWriter success");
        } catch(e:Exception) {
            Console.OUT.println("testSameReaderWriter failed");
        }
    }

    public static def testReaders() {
        val lock = new TxLockCREW();
        lock.lockRead(1);
        lock.lockRead(2);
        lock.lockRead(3);
        lock.unlock();
        lock.unlock();
        lock.unlock();
    }
    
    public static def testWriters() {
        val lock = new TxLockCREW();
        lock.lockWrite(1);
        lock.lockWrite(2);
        lock.unlock();
        lock.unlock();
    }
    

    public static def testReadersWriter() {
        val lock = new TxLockCREW();
        lock.lockRead(1);
        lock.lockRead(2);
        lock.lockRead(3);
        lock.lockWrite(3);
        lock.unlock();
        lock.unlock();
        lock.unlock();
    }
    
    public static def testSameReaderWriter() {
        val lock = new TxLockCREW();
        lock.lockRead(1);
        lock.lockWrite(1);
        lock.lockRead(1);
        lock.lockWrite(1);
        lock.unlock();
        lock.unlock();
        lock.unlock();
        lock.unlock();
    }
    
}