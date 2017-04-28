package zookeeperjk.testZK;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZookeeperTest {
	
	private static final int SESSION_TIMEOUT = 30000;
	
	public static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperTest.class);
	
	private Watcher watcher =  new Watcher() {

		public void process(WatchedEvent event) {
			LOGGER.info("process : " + event.getType());
		}
	};
	
	private ZooKeeper zooKeeper;
	
	/**
	 *  ����zookeeper
	 * @throws IOException
	 */
	@org.junit.Before
	public void connect() throws IOException {
		zooKeeper  = new ZooKeeper("hadoop1:2181,hadoop2:2181,hadoop3:2181", SESSION_TIMEOUT, watcher);
	}
	
	/**
	 *  �ر�����
	 */
	@After
	public void close() {
		try {
			zooKeeper.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * ����һ��znode 
	 *  1.CreateMode ȡֵ  
	 *  PERSISTENT���־û������Ŀ¼�ڵ�洢�����ݲ��ᶪʧ
	 *  PERSISTENT_SEQUENTIAL��˳���Զ���ŵ�Ŀ¼�ڵ㣬����Ŀ¼�ڵ����ݵ�ǰ�ѽ����ڵĽڵ����Զ��� 1��Ȼ�󷵻ظ��ͻ����Ѿ��ɹ�������Ŀ¼�ڵ�����
	 *  EPHEMERAL����ʱĿ¼�ڵ㣬һ����������ڵ�Ŀͻ�����������˿�Ҳ���� session���ڳ�ʱ�����ֽڵ�ᱻ�Զ�ɾ��
	 *  EPHEMERAL_SEQUENTIAL����ʱ�Զ���Žڵ�
	 * <br>------------------------------<br>
	 */
	@Test
	public void testCreate() {
		String result = null;
		 try {
			 result = zooKeeper.create("/zk", "zk001data".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (Exception e) {
			 LOGGER.error(e.getMessage());
			 Assert.fail();
		}
		 LOGGER.info("create result : {}", result);
	 }
	
	/**
	 * ɾ���ڵ�  ���԰汾
	 */
	@Test
	public void testDelete() {
		 try {
			zooKeeper.delete("/zk001", -1);
		} catch (Exception e) {
			 LOGGER.error(e.getMessage());
			 Assert.fail();
		}
	}
	
	/**
	 *   ��ȡ����
	 */
	@Test
	public void testGetData() {
		String result = null;
		 try {
			 byte[] bytes = zooKeeper.getData("/zk001", null, null);
			 result = new String(bytes);
		} catch (Exception e) {
			 LOGGER.error(e.getMessage());
			 Assert.fail();
		}
		 LOGGER.info("getdata result : {}", result);
	}
	
	/**
	 *   ��ȡ����  ����watch
	 */
	@Test
	public void testGetDataWatch() {
		String result = null;
		 try {
			 byte[] bytes = zooKeeper.getData("/zk001", new Watcher() {
				public void process(WatchedEvent event) {
					LOGGER.info("testGetDataWatch  watch : {}", event.getType());
				}
			 }, null);
			 result = new String(bytes);
		} catch (Exception e) {
			 LOGGER.error(e.getMessage());
			 Assert.fail();
		}
		 LOGGER.info("getdata result : {}", result);
		 
		 // ����wacth  NodeDataChanged
		 try {
			 zooKeeper.setData("/zk001", "testSetData".getBytes(), -1);
		} catch (Exception e) {
			 LOGGER.error(e.getMessage());
			 Assert.fail();
		}
	}
	
	/**
	 *    �жϽڵ��Ƿ����
	 *    �����Ƿ������Ŀ¼�ڵ㣬����� watcher ���ڴ��� ZooKeeperʵ��ʱָ���� watcher
	 */
	@Test
	public void testExists() {
		Stat stat = null;
		 try {
			 stat = zooKeeper.exists("/zk001", false);
		} catch (Exception e) {
			 LOGGER.error(e.getMessage());
			 Assert.fail();
		}
		 Assert.assertNotNull(stat);
		 LOGGER.info("exists result : {}", stat.getCzxid());
	}
	
	/**
	 *     ���ö�Ӧznode�µ�����  ,  -1��ʾƥ�����а汾
	 */
	@Test
	public void testSetData() {
		Stat stat = null;
		 try {
			 stat = zooKeeper.setData("/zk001", "testSetData".getBytes(), -1);
		} catch (Exception e) {
			 LOGGER.error(e.getMessage());
			 Assert.fail();
		}
		 Assert.assertNotNull(stat);
		 LOGGER.info("exists result : {}", stat.getVersion());	
	}
	
	/**
	 *    �жϽڵ��Ƿ����, 
	 *    �����Ƿ������Ŀ¼�ڵ㣬����� watcher ���ڴ��� ZooKeeperʵ��ʱָ���� watcher
	 */
	@Test
	public void testExistsWatch1() {
		Stat stat = null;
		 try {
			 stat = zooKeeper.exists("/zk001", true);
		} catch (Exception e) {
			 LOGGER.error(e.getMessage());
			 Assert.fail();
		}
		 Assert.assertNotNull(stat);
		 
		 try {
			zooKeeper.delete("/zk001", -1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 *    �жϽڵ��Ƿ����, 
	 *    ���ü�����Ŀ¼�ڵ�� Watcher
	 */
	@Test
	public void testExistsWatch2() {
		Stat stat = null;
		 try {
			 stat = zooKeeper.exists("/zk002", new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					LOGGER.info("testExistsWatch2  watch : {}", event.getType());
				}
			 });
		} catch (Exception e) {
			 LOGGER.error(e.getMessage());
			 Assert.fail();
		}
		 Assert.assertNotNull(stat);
		 
		 // ����watch �е�process����   NodeDataChanged
		 try {
			zooKeeper.setData("/zk002", "testExistsWatch2".getBytes(), -1);
		} catch (Exception e) {
			e.printStackTrace();
		}
		 
		 // ���ᴥ��watch ֻ�ᴥ��һ��
		 try {
			zooKeeper.delete("/zk002", -1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 *  ��ȡָ���ڵ��µ��ӽڵ�
	 */
	@Test
	public void testGetChild() {
		 try {
			 zooKeeper.create("/zk/001", "001".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			 zooKeeper.create("/zk/002", "002".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			 
			 List<String> list = zooKeeper.getChildren("/zk", true);
			for (String node : list) {
				LOGGER.info("fffffff {}", node);
			}
		} catch (Exception e) {
			 LOGGER.error(e.getMessage());
			 Assert.fail();
		}
	}
}
