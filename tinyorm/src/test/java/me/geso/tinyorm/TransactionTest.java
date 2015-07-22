package me.geso.tinyorm;

import static org.junit.Assert.assertEquals;

import net.moznion.db.transaction.manager.TransactionScope;

import lombok.Data;
import lombok.EqualsAndHashCode;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;

import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Optional;

public class TransactionTest extends TestBase {
	@Before
	public void before() throws SQLException {
		orm.getConnection().setAutoCommit(true);
		orm.getConnection().prepareStatement("DROP TABLE IF EXISTS x")
			.executeUpdate();
		orm.getConnection()
			.prepareStatement("CREATE TABLE x (a VARCHAR(255) NOT NULL, n int default 0, PRIMARY KEY (a))")
			.executeUpdate();
	}

	@Test
	public void transactionCommitSuccessfully() throws SQLException {
		orm.transactionBegin();
		orm.insert(X.class).value("a", "hoge").execute();
		orm.transactionCommit();
		Optional<X> row = orm.single(X.class).where("a=?", "hoge").execute();
		assertEquals(row.get().getA(), "hoge");
	}

	@Test
	public void transactionRollbackSuccessfully() throws SQLException {
		orm.transactionBegin();
		orm.insert(X.class).value("a", "hoge").execute();
		orm.transactionRollback();
		Optional<X> row = orm.single(X.class).where("a=?", "hoge").execute();
		assertEquals(row.isPresent(), false);
	}

	@Test
	public void transactionCommitWithScopeSuccessfully() throws SQLException {
		try (TransactionScope txn = orm.createTransactionScope()) {
			orm.insert(X.class).value("a", "hoge").execute();
			orm.transactionCommit();
		}
		Optional<X> row = orm.single(X.class).where("a=?", "hoge").execute();
		assertEquals(row.get().getA(), "hoge");
	}

	@Test
	public void transactionRollbackWithScopeSuccessfully() throws SQLException {
		try (TransactionScope txn = orm.createTransactionScope()) {
			orm.insert(X.class).value("a", "hoge").execute();
			orm.transactionRollback();
		}
		Optional<X> row = orm.single(X.class).where("a=?", "hoge").execute();
		assertEquals(row.isPresent(), false);
	}

	@Test
	public void transactionImplicitRollbackWithScopeSuccessfully()
			throws SQLException {
		try (TransactionScope txn = orm.createTransactionScope()) {
			orm.insert(X.class).value("a", "hoge").execute();
		}
		Optional<X> row = orm.single(X.class).where("a=?", "hoge").execute();
		assertEquals(row.isPresent(), false);
	}

	@Table("x")
	@Data
	@EqualsAndHashCode(callSuper = false)
	public static class X extends Row<X> {
		@PrimaryKey
		private String a;
	}
}
