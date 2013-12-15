package net.bramp.db_patterns;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import javax.sql.DataSource;

import net.bramp.sql.ResultSets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses the MySQL sleep() / kill to implement a distributed Condition
 * 
 * @author bramp
 *
 */
public class MySQLSleepBasedCondition implements Condition {

	final static Logger LOG = LoggerFactory.getLogger(MySQLSleepBasedCondition.class);

	final static long DEFAULT_WAIT = 60000000000L;

	final static String sleepQuery = "SELECT SLEEP(?), ?;";
	final static String wakeQuery = "KILL QUERY ?;";

	final static String listQuery_new =  // MySQL 5.1.7 or newer
		"SELECT Id, User, Host, Db, Command, Time, State, Info FROM " +
		"INFORMATION_SCHEMA.PROCESSLIST " +
		"WHERE STATE = 'User sleep' AND INFO LIKE ?" +
		"ORDER BY TIME";

	final static String listQuery_old = "SHOW PROCESSLIST;";

	final DataSource ds;
	final String lockName;

	final static ResultSetFilter.Predicate isOurLockPredicate = new ResultSetFilter.Predicate() {

		public boolean apply(ResultSet rs) throws SQLException {
			if (LOG.isDebugEnabled()) {
				LOG.debug("ResultSet {}", ResultSets.toString(rs));
			}

			String state = rs.getString("State");
			if (!state.equals("User sleep"))
				return false;

			String info = rs.getString("Info");
			// TODO check info looks like:
			//SELECT SLEEP(?), '?'
			return true;
		}
	};
	
	public MySQLSleepBasedCondition(DataSource ds, String lockName) {
		this.ds = ds;
		this.lockName = lockName;
		
		// TODO Detect MySQL version
		
	}

	/**
	 * 
	 * @param nanosTimeout
	 * @return true if correctly awaked, false if timeout
	 * @throws InterruptedException
	 */
	protected boolean awaitNanosInternal(long nanosTimeout) throws InterruptedException {
		try {
			Connection c = ds.getConnection();
			try {
				PreparedStatement s = c.prepareStatement(sleepQuery);
				s.setFloat(1, nanosTimeout / 1000000000L);
				s.setString(2, lockName);
				s.execute();

				ResultSet rs = s.getResultSet();
				if (rs.next())
					return rs.getInt(1) == 1;

				return true;

			} finally {
				c.close();
			}

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public long awaitNanos(long nanosTimeout) throws InterruptedException {
		long now = System.nanoTime();
		awaitNanosInternal(nanosTimeout);
		return System.nanoTime() - now;
	}

	public void await() throws InterruptedException {
		while (!awaitNanosInternal(DEFAULT_WAIT)) {
			// Keep looping, until we expire before our timeout
			// TODO There is a race condition here. Between iterations we might miss a wakeup
		}
	}

	public void awaitUninterruptibly() {
		while (true) {
			try {
				await();
				break;
			} catch (InterruptedException e) {}
		}
	}

	public boolean await(long time, TimeUnit unit) throws InterruptedException {
		return awaitNanos(unit.toNanos(time)) > 0;
	}

	public boolean awaitUntil(Date deadline) throws InterruptedException {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Get a list of the other threads waiting
	 * @throws SQLException 
	 */
	protected ResultSet findLockThreads(Connection c) throws SQLException {
		PreparedStatement s = c.prepareStatement(listQuery_new);
		s.setString(1, "%" + lockName + "%");
		return new ResultSetFilter(s.executeQuery(), isOurLockPredicate);
	}

	protected void killThread(Connection c, long threadId) throws SQLException {
		LOG.debug("Killing thread {}", threadId);

		PreparedStatement s = c.prepareStatement(wakeQuery);
		s.setLong(1, threadId);
		s.execute();
	}
	
	/**
	 * Will signal the thread that's been waiting the longest
	 */
	public void signal() {
		try {
			Connection c = ds.getConnection();
			try {
				// Find a list of blocked threads to wake up
				ResultSet threads = findLockThreads(c);
				if (!threads.next()) {
					LOG.debug("Nothing to wake up, lets bail");
					return;
				}
				long toWake = threads.getLong("Id");
				threads.close();

				killThread(c, toWake);

			} finally {
				c.close();
			}

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public void signalAll() {
		try {
			Connection c = ds.getConnection();
			try {
				// Find a list of blocked threads to wake up
				List<Long> toWake = new ArrayList<Long>();
				ResultSet threads = findLockThreads(c);
				while(threads.next()) {
					toWake.add( threads.getLong("Id") );
				}
				threads.close();

				for (Long id : toWake) {
					killThread(c, id);
				}

			} finally {
				c.close();
			}

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
