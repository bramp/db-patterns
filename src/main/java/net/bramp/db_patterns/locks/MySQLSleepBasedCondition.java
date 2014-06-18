package net.bramp.db_patterns.locks;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import javax.annotation.Nonnull;
import javax.sql.DataSource;

import net.bramp.sql.ResultSetFilter;
import net.bramp.sql.ResultSets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses the MySQL sleep() / kill to implement a distributed Condition
 *
 * @author bramp
 */
public class MySQLSleepBasedCondition implements Condition {

	final static Logger LOG = LoggerFactory.getLogger(MySQLSleepBasedCondition.class);

	static long DEFAULT_WAIT = 60000000000L;

	final static String sleepQuery = "SELECT SLEEP(?), ?;";
	final static String wakeQuery = "KILL QUERY ?;";

	final static String listQueryNew =  // MySQL 5.1.7 or newer
		"SELECT Id, User, Host, Db, Command, Time, State, Info FROM " +
		"INFORMATION_SCHEMA.PROCESSLIST " +
		"WHERE STATE = 'User sleep' AND INFO LIKE ? " +
		"ORDER BY TIME";

	final static String listQueryOld = "SHOW PROCESSLIST;";

	final boolean useListQueryNew = false;

	final DataSource ds;
	final String lockName;

	final ResultSetFilter.Predicate isOurLockPredicate = new ResultSetFilter.Predicate() {

		public boolean apply(ResultSet rs) throws SQLException {
			if (LOG.isDebugEnabled()) {
				LOG.debug("ResultSet {}", ResultSets.toString(rs));
			}

			String state = rs.getString(7);
			if (state != null && !state.equals("User sleep"))
				return false;

			String info = rs.getString(8);
			return info != null && info.matches("SELECT SLEEP\\([\\d.]+\\), '" + lockName + "'");
		}
	};

	public MySQLSleepBasedCondition(@Nonnull DataSource ds, @Nonnull String lockName) {
		this.ds = ds;
		this.lockName = lockName;

		// TODO Detect MySQL version (update useListQueryNew)
		// TODO Detect if we can sleep/kill
	}

	/**
	 * @param nanosTimeout The number of nanoseconds to wait
	 * @return true if awaken (correctly, or spuriously), false if timeout
	 * @throws InterruptedException
	 */
	protected boolean awaitNanosInternal(long nanosTimeout) throws InterruptedException {
		if (nanosTimeout <= 0)
			return false;

		long now = System.nanoTime();

		try {
			Connection c = ds.getConnection();
			try {
				PreparedStatement s = c.prepareStatement(sleepQuery);
				try {

					// Adjust nanosTimeout (due to time it took to get a connection)
					nanosTimeout -= (System.nanoTime() - now);

					// Convert to seconds, but round to whole number of milliseconds
					s.setFloat(1, Math.round(nanosTimeout / 1000000.0) / 1000f);
					s.setString(2, lockName);
					s.execute();

					ResultSet rs = s.getResultSet();
					if (rs != null && rs.next())
						return rs.getInt(1) == 1;

					return true;

				} finally {
					s.close();
				}

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
			// Keep looping, until we expire before our timeout or are interuptted
			if (Thread.interrupted())
				throw new InterruptedException();

			// TODO There is a race condition here. Between iterations we might miss a wakeup
		}
	}

	public void awaitUninterruptibly() {
		while (true) {
			try {
				await();
				break;
			} catch (InterruptedException e) {
			}
		}
	}

	public boolean await(long time, TimeUnit unit) throws InterruptedException {
		return awaitNanosInternal(unit.toNanos(time));
	}

	public boolean awaitUntil(Date deadline) throws InterruptedException {
		long duration = deadline.getTime() - System.currentTimeMillis();
		return awaitNanosInternal(TimeUnit.MILLISECONDS.toNanos(duration));
	}

	/**
	 * Get a list of the other threads waiting
	 *
	 * @throws SQLException
	 */
	protected ResultSet findLockThreads(@Nonnull Connection c) throws SQLException {
		PreparedStatement s = null;

		if (useListQueryNew) {
			s = c.prepareStatement(listQueryNew);
			s.setString(1, "SELECT SLEEP(%" + lockName + "%");
		} else {
			s = c.prepareStatement(listQueryOld);
		}
		return new ResultSetFilter(s.executeQuery(), isOurLockPredicate);
	}

	protected void killThread(@Nonnull Connection c, long threadId) throws SQLException {
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
					LOG.debug("Nothing to wake up for '{}'", lockName);
					return;
				}
				long toWake = threads.getLong(1);
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
				while (threads.next()) {
					toWake.add(threads.getLong(1));
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
