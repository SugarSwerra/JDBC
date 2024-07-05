package com.esame.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.mysql.cj.jdbc.MysqlDataSource;



public class Database {
	
	private Connection con;
	
	public static void main(String[] args) {
		
		Database ibm = new Database();
		
		try {
			if (ibm.startConnection(null).isValid(100)) {
				
				String nomeDB = "Esame_JBDC";
				
				ibm.startConnection(null);
				System.out.println("Connessione con il SERVER stabilita con successo");
				System.out.println("--------------------------");
				
				
				ibm.createSchema(nomeDB);
				System.out.println("Database presente nel Server");
				System.out.println("--------------------------");
				
				ibm.useDB(nomeDB);
				System.out.println("Database selezionato con successo");
				System.out.println("--------------------------");
				
//				ibm.closeConnection();
//				System.out.println("Connessione con il server terminata con successo");
//				System.out.println("--------------------------");
//				
//				ibm.startConnection(nomeDB);
//				System.out.println("Connessione con il DB avvenuta con successo");
//				System.out.println("--------------------------");
				
				
				
				ibm.createTable(nomeDB,"utente", "id", "cognome", "nome" );
				System.out.println("Tabella utente presente nel database");
				System.out.println("--------------------------");
				
				
				/*Inserimento di alcuni dati nella tabella utente
				 
				ibm.insertInfo(nomeDB, 1, "rossi", "mario");
				ibm.insertInfo(nomeDB, 2, "verdi", "andrea");
				ibm.insertInfo(nomeDB, 3, "bianchi", "massimo");
				ibm.insertInfo(nomeDB, 4, "vallieri", "sara");
				ibm.insertInfo(nomeDB, 5, "graviglia", "marco");
				ibm.insertInfo(nomeDB, 6, "esposito", "marzia"); */
				
				ibm.getBooksChronological(nomeDB);
				
				ibm.getMoreReaders(nomeDB);
				
				ibm.missingBooks(nomeDB);
				
				ibm.borrowedBooks(nomeDB, "vallieri", "1999-01-01", "2015-12-31");
				
				ibm.topThreeBooks(nomeDB);
				
				ibm.borrowedBooksFifteen(nomeDB);
				
				
			} 
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		
	}
	
	
	private Connection startConnection(String nomeDB) throws SQLException {
		
		MysqlDataSource dataSource = new MysqlDataSource();
		
		if (con == null) {
			
			dataSource.setServerName("127.0.0.1");
			dataSource.setPortNumber(3306);
			dataSource.setUser("root");
			dataSource.setPassword("admin");

			dataSource.setDatabaseName(nomeDB);
			
			con = dataSource.getConnection();
		}
		
		return con;
		
	}
	
	private void closeConnection() throws SQLException {
		MysqlDataSource dataSource = new MysqlDataSource();
		
		if (con == null) {
			
			dataSource.setServerName("127.0.0.1");
			dataSource.setPortNumber(3306);
			dataSource.setUser("root");
			dataSource.setPassword("admin");
			
			con = dataSource.getConnection();
			
			con.close();
		}
		
	}
	
	private void createSchema(String nameDB) throws SQLException {
		
		String sql = "CREATE SCHEMA IF NOT EXISTS `" +nameDB +"`";
		
		PreparedStatement ps = startConnection(null).prepareStatement(sql);
		
		ps.executeUpdate();
		ps.close();
	}
	
	private void useDB(String nomeDB) throws SQLException {
		
		String sql = "USE "+nomeDB+";";
		PreparedStatement ps = startConnection(null).prepareStatement(sql);
		
		ps.executeUpdate();
		ps.close();
	}
	
	private void createTable (String nomeDB, String nome_tabella, String colonna1, String colonna2, String colonna3) throws SQLException {
		
		String sql = "CREATE TABLE IF NOT EXISTS utente (" +colonna1+ " INTEGER NOT NULL PRIMARY KEY," +colonna2+ " VARCHAR(16) NOT NULL," +colonna3+ " VARCHAR(16) NOT NULL);";
		
		PreparedStatement ps = startConnection(nomeDB).prepareStatement(sql);
		
		ps.executeUpdate();
		ps.close();
		
	}
	
	private void insertInfo(String nomeDB, int id, String cognome, String nome) throws SQLException {
		
		String sql = "INSERT INTO utente(id,cognome,nome) VALUES (?, ?, ?)" ;
		
		PreparedStatement ps = startConnection(nomeDB).prepareStatement(sql);
		
		ps.setInt(1, id);
		ps.setString(2, cognome);
		ps.setString(3, nome);
		

		ps.executeUpdate();
		ps.close();
	}
	
	private void getBooksChronological(String nameDB) throws SQLException {
		
		String sql = "SELECT prestito.data_inizio, libro.titolo FROM libro"
				+ " INNER JOIN prestito ON prestito.id_l = libro.id"
				+ " INNER JOIN utente ON utente.id = prestito.id_u "
				+ " WHERE cognome = 'vallieri' ORDER BY data_inizio";
		
		PreparedStatement ps = startConnection(nameDB).prepareStatement(sql);
		
		ResultSet rs = ps.executeQuery();
		
		System.out.println("Libri presi in prestito dall'utente Vallieri:");
		
		while (rs.next()) {
			
			System.out.println("data_inizio: " + rs.getDate(1));
			System.out.println("titolo: " + rs.getString(2));
			System.out.println("--------------------------");
		}
	}
	
	private void getMoreReaders(String nameDB) throws SQLException {
		
		String sql = "SELECT utente.cognome, COUNT(*) FROM utente"
				+ " INNER JOIN prestito ON prestito.id_u = utente.id"
				+ " GROUP BY nome ORDER BY COUNT(*) DESC LIMIT 3";
		
		PreparedStatement ps = startConnection(nameDB).prepareStatement(sql);
		
		ResultSet rs = ps.executeQuery();
		
		System.out.println("Top 3 lettori:");
		
		while (rs.next()) {
			
			System.out.println("Cognome: " + rs.getString(1));
			System.out.println("Numero libri letti: " + rs.getString(2));
			System.out.println("--------------------------");
		}
	}
	
	private void missingBooks(String nameDB) throws SQLException {
		
		String sql = "SELECT utente.*, titolo FROM utente"
				+ " INNER JOIN prestito ON prestito.id_u = utente.id"
				+ " INNER JOIN libro ON libro.id = prestito.id_l"
				+ " WHERE data_fine IS NULL";
		
		PreparedStatement ps = startConnection(nameDB).prepareStatement(sql);
		
		ResultSet rs = ps.executeQuery();
		
		System.out.println("Libri ancora non restituiti:");
		
		while (rs.next()) {
			
			System.out.println("Id Utente: " + rs.getInt(1));
			System.out.println("Cognome: " + rs.getString(2));
			System.out.println("Nome: " + rs.getString(3));
			System.out.println("Titolo libro: " + rs.getString(4));
			System.out.println("--------------------------");
		}
	}
	
	private void borrowedBooks(String nameDB, String cognome, String data_inizio, String data_fine) throws SQLException {
		
		String sql = "SELECT libro.*, data_inizio FROM libro"
				+ " INNER JOIN prestito ON prestito.id_u = libro.id"
				+ " INNER JOIN utente ON utente.id = prestito.id_u"
				+ " WHERE cognome = '" +cognome+ "' AND (data_inizio BETWEEN '" +data_inizio+ "'"
						+ " AND '" +data_fine+ "')";
		
		PreparedStatement ps = startConnection(nameDB).prepareStatement(sql);
		
		ResultSet rs = ps.executeQuery();
		
		System.out.println("Libri ancora non restituiti:");
		
		while (rs.next()) {
			
			System.out.println("Id Utente: " + rs.getInt(1));
			System.out.println("Cognome: " + rs.getString(2));
			System.out.println("Nome: " + rs.getString(3));
			System.out.println("Titolo libro: " + rs.getString(4));
			System.out.println("--------------------------");
		}
	}
	
	private void topThreeBooks(String nameDB) throws SQLException {
		
		String sql = "SELECT titolo, COUNT(*) FROM libro"
				+ " INNER JOIN prestito ON prestito.id_l = libro.id"
				+ " GROUP BY titolo ORDER BY COUNT(*) DESC";
		
		PreparedStatement ps = startConnection(nameDB).prepareStatement(sql);
		
		ResultSet rs = ps.executeQuery();
		
		System.out.println("Top 3 libri");
		
		while (rs.next()) {
			
			System.out.println("Titolo: " + rs.getString(1));
			System.out.println("Numero Copie Prestate: " + rs.getString(2));
		}
	}
	
	private void borrowedBooksFifteen(String nameDB) throws SQLException {
		
		String sql = "SELECT * FROM prestito"
				+ " WHERE DATEDIFF(data_fine, data_inizio) > 15";
		
		PreparedStatement ps = startConnection(nameDB).prepareStatement(sql);
		
		ResultSet rs = ps.executeQuery();
		
		System.out.println("Libri prestati per più di 15 giorni:");
		
		while (rs.next()) {
			
			System.out.println("Titolo: " + rs.getString(1));
			System.out.println("Numero Data Inizio: " + rs.getDate(2));
			System.out.println("Numero Data Fine: " + rs.getDate(3));
		}
	}
	
	
	

}
