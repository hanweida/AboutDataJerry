package zookeeperjk.testZK;
import java.util.Arrays;  
import java.util.List;  
import org.apache.zookeeper.CreateMode;  
import org.apache.zookeeper.KeeperException;  
import org.apache.zookeeper.ZooDefs.Ids;  
  
public class ZooKeeperOperator extends AbstractZooKeeper {  
    /** 
     * �����־�̬��znode,��֧�ֶ�㴴��.�����ڴ���/parent/child�������,��/parent.�޷�ͨ��. 
     * @param path eg:  /parent/child1 
     * @param data 
     * @throws InterruptedException  
     * @throws KeeperException  
     */  
    public void create(String path,byte[] data) throws KeeperException, InterruptedException{  
        this.zooKeeper.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT/*�˴�������Ϊ�־�̬�Ľڵ�,��Ϊ˲̬*/);  
    }  
      
      
    /** 
     * ��ȡ�ڵ�ĺ�����Ϣ 
     * @param path 
     * @throws KeeperException 
     * @throws InterruptedException 
     */  
    public void getChild(String path) throws KeeperException, InterruptedException{  
        try {  
            List<String> children = this.zooKeeper.getChildren(path, false);  
            if (children.isEmpty()) {  
                System.out.printf("û�нڵ���%s��.", path);  
                return;  
            }else{  
                System.out.printf("�ڵ�%s�д��ڵĽڵ�:\n", path);  
                for(String child: children){  
                    System.out.println(child);  
                }  
            }  
        } catch (KeeperException.NoNodeException e) {  
            System.out.printf("%s�ڵ㲻����.", path);  
            throw e;  
        }  
    }  
  
    public byte[] getData(String path) throws KeeperException, InterruptedException {  
        return  this.zooKeeper.getData(path, false,null);  
    }  
    
    
    public static void main(String[] args) {  
        try {  
            ZooKeeperOperator zkoperator             = new ZooKeeperOperator();  
            zkoperator.connect("192.168.1.201");  
            byte[] data = new byte[]{'d','a','t','a'};  
              
            zkoperator.create("/root",null);  
            System.out.println(Arrays.toString(zkoperator.getData("/root")));  
              
            zkoperator.create("/root/child1",data);  
            System.out.println(Arrays.toString(zkoperator.getData("/root/child1")));  
              
            zkoperator.create("/root/child2",data);  
            System.out.println(Arrays.toString(zkoperator.getData("/root/child2")));  
              
            System.out.println("�ڵ㺢����Ϣ:");  
            zkoperator.getChild("/root");  
              
            zkoperator.close();  
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
    } 
}  