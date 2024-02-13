package dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;

import model.Card;
import model.Player;

public class DaoImpl implements Dao{

	private Connection conexion;

	// Constantes. Deber�an estar en una clase de constanes en el pck utils
	public static final String SCHEMA_NAME = "java_bbdd";
	public static final String CONNECTION = "jdbc:mysql://localhost:3306/" + SCHEMA_NAME
			+ "?useSSL=false&useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
	public static final String USER_CONNECTION = "root";
	public static final String PASS_CONNECTION = "localhost";

	public static final String GET_USER_PASS = "SELECT * FROM player WHERE user = ? AND password = ?";
	

	public void connect() throws SQLException {
		String url = CONNECTION;
		String user = USER_CONNECTION;
		String pass = PASS_CONNECTION;
		conexion = DriverManager.getConnection(url, user, pass);
	}

	public void disconnect() throws SQLException {
		if (conexion != null) {
			conexion.close();
		}
	}

	public int getLastIdCard(int playerId) throws SQLException {
	    PreparedStatement preparedStatement = null;
	    ResultSet resultSet = null;
	    int lastId = 0;

	    try {
	        String query = "SELECT ifnull(max(id),0)+1 FROM CARD WHERE id_Player=?";
	        preparedStatement = conexion.prepareStatement(query);
	        preparedStatement.setInt(1, playerId);
	        resultSet = preparedStatement.executeQuery();

	        if (resultSet.next()) {
	            lastId = resultSet.getInt(1);
	        }
	    } catch (SQLException e) {
	        throw new SQLException("Error al obtener el último ID de la carta", e);
	    } finally {
	        if (resultSet != null) {
	            resultSet.close();
	        }
	        if (preparedStatement != null) {
	            preparedStatement.close();
	        }
	    }

	    return lastId;
	}


	/**
	 * get object last Card played from game join card table
	 * 
	 * @return last card played
	 * @throws SQLException
	 */
	public Card getLastCard() throws SQLException {
	    PreparedStatement preparedStatement = null;
	    ResultSet resultSet = null;
	    Card lastCard = null;

	    try {
	        String query = "SELECT Id_Card FROM GAME WHERE id=(SELECT MAX(id) FROM GAME)";
	        preparedStatement = conexion.prepareStatement(query);
	        resultSet = preparedStatement.executeQuery();

	        if (resultSet.next()) {
	            int idCard = resultSet.getInt("Id_Card");
	            
	            lastCard = getCard(idCard); 
	        }
	    } catch (SQLException e) {
	        throw new SQLException("Error al obtener la última carta", e);
	    } finally {
	        if (resultSet != null) {
	            resultSet.close();
	        }
	        if (preparedStatement != null) {
	            preparedStatement.close();
	        }
	    }

	    return lastCard;
	}



	/**
	 * get object Player by user and password from player table
	 * 
	 * @param user
	 * @param pass
	 * @return
	 * @throws SQLException
	 */
	public Player getPlayer(String user, String pass) throws SQLException {
	    Player player = null;
	    PreparedStatement preparedStatement = null;
	    ResultSet resultSet = null;

	    try {
	        String query = GET_USER_PASS;
	        preparedStatement = conexion.prepareStatement(query);
	        preparedStatement.setString(1, user);
	        preparedStatement.setString(2, pass);
	        resultSet = preparedStatement.executeQuery();

	        if (resultSet.next()) {
	            int id = resultSet.getInt("id");
	            String name = resultSet.getString("name");
	            int games = resultSet.getInt("games");
	            int victories = resultSet.getInt("victories");

	            player = new Player(id, name, games, victories);
	        }
	    } catch (SQLException e) {
	        throw new SQLException("Error al obtener el jugador", e);
	    } finally {
	        if (resultSet != null) {
	            resultSet.close();
	        }
	        if (preparedStatement != null) {
	            preparedStatement.close();
	        }
	    }

	    return player;
	}



	/**
	 * @param id player
	 * @return list of hand cards, they are in card table but they are not in game
	 *         table(played)
	 * @throws SQLException
	 */
	public ArrayList<Card> getCards(int playerId) throws SQLException {
	    ArrayList<Card> cards = new ArrayList<>();
	    PreparedStatement preparedStatement = null;
	    ResultSet resultSet = null;

	    try {
	        String query = "SELECT * FROM CARD LEFT JOIN GAME ON card.id = game.id_Card WHERE id_Player=? AND game.id is null";
	        preparedStatement = conexion.prepareStatement(query);
	        preparedStatement.setInt(1, playerId);
	        resultSet = preparedStatement.executeQuery();

	        while (resultSet.next()) {
	            int id = resultSet.getInt("id");
	            String number = resultSet.getString("number");
	            String color = resultSet.getString("color");

	            Card card = new Card(id, number, color, playerId);
	            cards.add(card);
	        }
	    } catch (SQLException e) {
	        throw new SQLException("Error al obtener las cartas", e);
	    } finally {
	        if (resultSet != null) {
	            resultSet.close();
	        }
	        if (preparedStatement != null) {
	            preparedStatement.close();
	        }
	    }

	    return cards;
	}




	/**
	 * @param id card
	 * @return object card from card table by id_card
	 * @throws SQLException
	 */
	public Card getCard(int cardId) throws SQLException {
	    PreparedStatement preparedStatement = null;
	    ResultSet resultSet = null;
	    Card card = null;

	    try {
	        String query = "SELECT * FROM CARD WHERE id=?";
	        preparedStatement = conexion.prepareStatement(query);
	        preparedStatement.setInt(1, cardId);
	        resultSet = preparedStatement.executeQuery();

	        if (resultSet.next()) {
	            int id = resultSet.getInt("id");
	            String number = resultSet.getString("number");
	            String color = resultSet.getString("color");
	            int playerId = resultSet.getInt("playerId");
	            card = new Card(id, number, color, playerId);
	        }
	    } catch (SQLException e) {
	        throw new SQLException("Error al obtener la carta con id " + cardId, e);
	    } finally {
	        if (resultSet != null) {
	            resultSet.close();
	        }
	        if (preparedStatement != null) {
	            preparedStatement.close();
	        }
	    }

	    return card;
	}



	/**
	 * insert new game with cardId
	 * 
	 * @param card
	 * @throws SQLException
	 */
	public void saveGame(Card card) throws SQLException {
	    PreparedStatement preparedStatement = null;

	    try {
	        String query = "INSERT INTO GAME (id_card) VALUES (?)";
	        preparedStatement = conexion.prepareStatement(query);
	        preparedStatement.setInt(1, card.getId());
	        preparedStatement.executeUpdate();
	    } catch (SQLException e) {
	        throw new SQLException("Error al guardar la carta en el juego", e);
	    } finally {
	        if (preparedStatement != null) {
	            preparedStatement.close();
	        }
	    }
	}


	/**
	 * insert new card with card fields
	 * 
	 * @param card
	 * @throws SQLException
	 */
	public void saveCard(Card card) throws SQLException {
	    PreparedStatement preparedStatement = null;

	    try {
	        String query = "INSERT INTO CARD (id_Player, number, color) VALUES (?, ?, ?)";
	        preparedStatement = conexion.prepareStatement(query);
	        preparedStatement.setInt(1, card.getPlayerId());
	        preparedStatement.setString(2, card.getNumber());
	        preparedStatement.setString(3, card.getColor());

	        preparedStatement.executeUpdate();
	    } catch (SQLException e) {
	        throw new SQLException("Error al guardar la carta", e);
	    } finally {
	        if (preparedStatement != null) {
	            preparedStatement.close();
	        }
	    }
	}


	/**
	 * delete last card from game table if it was a end game card(change side or
	 * skip)
	 * 
	 * @param card
	 * @throws SQLException
	 */
	public void deleteCard(Card card) throws SQLException {
	    PreparedStatement preparedStatement = null;

	    try {
	        String query = "DELETE FROM CARD WHERE id=?";
	        preparedStatement = conexion.prepareStatement(query);
	        preparedStatement.setInt(1, card.getId());
	        preparedStatement.executeUpdate();
	    } catch (SQLException e) {
	        throw new SQLException("Error al eliminar la carta", e);
	    } finally {
	        if (preparedStatement != null) {
	            preparedStatement.close();
	        }
	    }
	}

	/**
	 * delete all records from card and game tables
	 * 
	 * @throws SQLException
	 */
	public void clearDeck(int playerId) throws SQLException {
	    PreparedStatement preparedStatement = null;

	    try {
	        String queryCard = "DELETE FROM CARD WHERE id_player=?";
	        preparedStatement = conexion.prepareStatement(queryCard);
	        preparedStatement.setInt(1, playerId);
	        preparedStatement.executeUpdate();
	        
	        preparedStatement = null;
	        
	        String queryGame = "DELETE FROM GAME";
	        preparedStatement = conexion.prepareStatement(queryGame);
	        preparedStatement.executeUpdate();
	        
	    } catch (SQLException e) {
	        throw new SQLException("Error al limpiar el mazo del jugador", e);
	    } finally {
	        if (preparedStatement != null) {
	            preparedStatement.close();
	        }
	    }
	}

	/**
	 * update victories field if the game ends successfully using player id
	 * 
	 * @param playerId
	 * @throws SQLException
	 */
	public void addVictories(int playerId) throws SQLException {
	    PreparedStatement preparedStatement = null;

	    try {
	        String query = "UPDATE PLAYER SET victories = victories + 1 WHERE id=?";
	        preparedStatement = conexion.prepareStatement(query);
	        preparedStatement.setInt(1, playerId);
	        preparedStatement.executeUpdate();
	    } catch (SQLException e) {
	        throw new SQLException("Error al incrementar las victorias del jugador", e);
	    } finally {
	        if (preparedStatement != null) {
	            preparedStatement.close();
	        }
	    }
	}

	/**
	 * update games field if the game ends using player id
	 * 
	 * @param playerId
	 * @throws SQLException
	 */
	public void addGames(int playerId) throws SQLException {
	    PreparedStatement preparedStatement = null;

	    try {
	        String query = "UPDATE PLAYER SET games = games + 1 WHERE id=?";
	        preparedStatement = conexion.prepareStatement(query);
	        preparedStatement.setInt(1, playerId);
	        preparedStatement.executeUpdate();
	        
	    } catch (SQLException e) {
	        throw new SQLException("Error al incrementar los juegos del jugador", e);
	    } finally {
	        if (preparedStatement != null) {
	            preparedStatement.close();
	        }
	    }
	}

}
