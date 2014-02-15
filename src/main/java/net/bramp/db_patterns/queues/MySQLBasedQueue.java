package net.bramp.db_patterns.queues;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bramp.db_patterns.locks.MySQLSleepBasedCondition;

/**
 * A queue backed by MySQL
 * 
 * CREATE TABLE queue (
 *     id          INT UNSIGNED NOT NULL AUTO_INCREMENT,
 *     queue_name  VARCHAR(255) NOT NULL,    -- Queue name
 *     inserted    TIMESTAMP NOT NULL,      -- Time the row was inserted
 *     inserted_by VARCHAR(255) NOT NULL,    -- and by who
 *     acquired    TIMESTAMP NULL,          -- Time the row was acquired
 *     acquired_by VARCHAR(255) NULL,        -- and by who
 *     value       BLOB NOT NULL,           -- The actual data
 *     PRIMARY KEY (id)
 * ) ENGINE=INNODB DEFAULT CHARSET=UTF8;
 * 
 * TODO Create efficient drainTo
 * 
 * @author bramp
 *
 * @param <E>
 */
public class MySQLBasedQueue<E> extends AbstractBlockingQueue<E> {

	final static Logger LOG = LoggerFactory.getLogger(MySQLBasedQueue.class);

	final static String addQuery  = "INSERT INTO queue (queue_name, inserted, inserted_by, value) values (?, now(), ?, ?)";
	final static String peekQuery = "SELECT value FROM queue WHERE acquired IS NULL AND queue_name = ?  ORDER BY id ASC LIMIT 1";
	final static String sizeQuery = "SELECT COUNT(*) FROM queue WHERE acquired IS NULL AND queue_name = ?";

	/**
	 * Claims one row (and keeps it in the database)
	 */
	final static String pollQuery[] = {
		"SET @update_id := -1; ",

		"UPDATE queue SET " +
		"   id = (SELECT @update_id := id), " +
		"   acquired = NOW(), " +
		"   acquired_by = ? " +
		"WHERE acquired IS NULL AND queue_name = ? " +
		"ORDER BY id ASC " +
		"LIMIT 1; ",

		"SELECT value FROM queue WHERE id = @update_id"
	};

	final static String cleanupQuery =
		"DELETE FROM queue " +
		"WHERE acquired IS NOT NULL " +
		"   AND queue_name = ? " +
		"   AND acquired < DATE_SUB(NOW(), INTERVAL 10 DAY)";

	final static String cleanupAllQuery =
        "DELETE FROM queue " +
        "WHERE acquired IS NOT NULL " +
        "   AND acquired < DATE_SUB(NOW(), INTERVAL 10 DAY)";

	final String me;
	
	final DataSource ds;
	final String queueName;
	final Class<E> type;

	final Condition condition;

	public MySQLBasedQueue(DataSource ds, String queueName, Class<E> type) {
		this.ds = ds;
		this.queueName = queueName;
		this.type = type;
		this.condition = new MySQLSleepBasedCondition(ds, "queue-" + queueName);
		this.me = "Andrew";
	}

	public boolean add(E value) {
		try {
			Connection c = ds.getConnection();
			try {
				PreparedStatement s = c.prepareStatement(addQuery);
				s.setString(1, queueName);
				s.setObject(2, me); // Inserted by me
				s.setObject(3, value);
				s.execute();

				// Wake up one
				condition.signal();

				return true;

			} finally {
				c.close();
			}

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * No blocking
	 */
	public E peek() {
		try {
			Connection c = ds.getConnection();
			try {
				PreparedStatement s = c.prepareStatement(peekQuery);
				s.setString(1, queueName);
				if (s.execute()) {
					ResultSet rs = s.getResultSet();
					if (rs != null && rs.next()) {
						return rs.getObject(1, type);
					}
				}

				return null;

			} finally {
				c.close();
			}

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * No blocking
	 */
	public E poll() {
		try {
			Connection c = ds.getConnection();
			try {
				c.setAutoCommit(false);

				CallableStatement s1 = c.prepareCall(pollQuery[0]);
				s1.execute();

				PreparedStatement s2 = c.prepareStatement(pollQuery[1]);
				s2.setString(1, me); // Acquired by me
				s2.setString(2, queueName);
				s2.execute();

				CallableStatement s3 = c.prepareCall(pollQuery[2]);
				s3.execute();

				c.commit();

				if (s3.execute()) {
					ResultSet rs = s3.getResultSet();
					if (rs != null && rs.next()) {
						return rs.getObject(1, type);
					}
				}

				return null;

			} finally {
				c.setAutoCommit(true);
				c.close();
			}

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public int size() {
		try {
			Connection c = ds.getConnection();
			try {
				PreparedStatement s = c.prepareStatement(sizeQuery);
				s.setString(1, queueName);
				s.execute();

				ResultSet rs = s.getResultSet();
				if (rs != null && rs.next())
					return rs.getInt(1);

				throw new RuntimeException("Failed to retreive size");

			} finally {
				c.close();
			}

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Blocks until something is in the queue, up to timeout
	 * null if timeout occurs
	 */
	public E poll(long timeout, TimeUnit unit) throws InterruptedException {

		final long deadlineMillis = System.currentTimeMillis() + unit.toMillis(timeout);
		final Date deadline = new Date(deadlineMillis);

		E head = null;
		boolean stillWaiting = true;

		while (stillWaiting) {	
			// Check if we can grab one
			head = poll();
			if (head != null)
				break;

			// Block until we are woken, or deadline
			// Because we don't have a distributed lock around this condition, there is a race condition
			// whereby we might miss a notify(). However, we can somewhat mitigate the problem, by using
			// this in a polling fashion
			stillWaiting = condition.awaitUntil(deadline);
		}

		return head;
	}
	
	public void cleanup() throws SQLException {
		Connection c = ds.getConnection();
		try {
			CallableStatement s = c.prepareCall(cleanupQuery);
			s.setString(1, queueName);
			s.execute();

		} finally {
			c.close();
		}		
	}

	/**
	 * Cleans up all queues
	 * @throws SQLException
	 */
	public void cleanupAll() throws SQLException {
		Connection c = ds.getConnection();
		try {
			CallableStatement s = c.prepareCall(cleanupAllQuery);
			s.execute();

		} finally {
			c.close();
		}		
	}
}
