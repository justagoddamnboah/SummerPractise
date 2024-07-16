import java.sql.*;

public class JDBCRunner {

    private static final String PROTOCOL = "jdbc:postgresql://";        // URL-prefix
    private static final String DRIVER = "org.postgresql.Driver";       // Driver name
    private static final String URL_LOCALE_NAME = "localhost/";         // ваш компьютер + порт по умолчанию

    private static final String DATABASE_NAME = "orders";          // имя базы

    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;       // полная ссылка на БД
    public static final String USER_NAME = "postgres";                  // имя пользователя
    public static final String DATABASE_PASS = "postgres";              // пароль

    public static void main(String[] args) {

        // проверка возможности подключения
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");

        // попытка открыть соединение с базой данных, которое java-закроет перед выходом из try-with-resources
        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {

            getGames(connection); System.out.println();

            getDevelopers(connection); System.out.println();

            getDLC(connection); System.out.println();

            gamesMotherland(connection); System.out.println();

            gamesFromOneDev(connection, "CD Projekt Red"); System.out.println(); // Получить игры от одних разработчиков

            gamesOnOnePlatform(connection, "PS3"); System.out.println();

            gameSearch(connection, "Stray"); System.out.println();

            sumHoursPlayedOnOnePlatform(connection, "PC"); System.out.println();

            //addGame(connection, "Spyro: Reignited Trilogy", "Toys for Bob", 2018, 36, "PC"); System.out.println(); // Добавить игру

            //correctHoursPlayed(connection, "Marvel's Spider-Man Remastered", 75); System.out.println();  // Отредактировать количество сыгранных часов

            getGamesOrderByHoursPlayedDesc(connection); System.out.println();

            //deleteGame(connection, 18); System.out.println();  // Удалить игру

            //addDeveloper(connection, "Ubisoft", "France"); System.out.println();

            //deleteDeveloper(connection, 13); System.out.println();

            //addDLC(connection, "Farewell of the White Wolf", 12); System.out.println();

            deleteDLC(connection, 13); System.out.println();

            baseGameDLC(connection); System.out.println();

        } catch (SQLException e) {
            // При открытии соединения, выполнении запросов могут возникать различные ошибки
            // Согласно стандарту SQL:2008 в ситуациях нарушения ограничений уникальности (в т.ч. дублирования данных)
            // возникают ошибки соответствующие статусу (или дочерние ему): SQLState 23000 - Integrity Constraint Violation
            if (e.getSQLState().startsWith("23")){
                System.out.println("Произошло дублирование данных");
            } else throw new RuntimeException(e);
        }
    }

    // Проверка окружения и доступа к базе данных
    public static void checkDriver () {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту!");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB () {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе");
            throw new RuntimeException(e);
        }
    }

    public static void getGames(Connection connection) throws SQLException {
        System.out.println("Библиотека игр");
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM games");
        ResultSet rs = statement.executeQuery();
        int count = 0;
        while(rs.next()) {
            count += 1;
            System.out.println(count + ". " + rs.getString(1) + " | Разработчик: " + rs.getString(2)
                    + " | Год выхода: " + rs.getString(3) + " | Часов сыграно: " + rs.getString(4)
                    + " | Платформа: " + rs.getString(5) + " | ID: " + rs.getString(6));
        }
    }

    public static void getDevelopers(Connection connection) throws SQLException {
        System.out.println("Разработчики");
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM developers");
        ResultSet rs = statement.executeQuery();
        int count = 0;
        while(rs.next()) {
            count += 1;
            System.out.println(count + ". " + rs.getString(2) + " | Офис: "
                    + rs.getString(3) + " | ID: " + rs.getString(1));
        }
    }

    public static void getDLC(Connection connection) throws SQLException {
        System.out.println("Дополнения");
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM dlc");
        ResultSet rs = statement.executeQuery();
        int count = 0;
        while(rs.next()) {
            count += 1;
            System.out.println(count + ". " + rs.getString(2) + " | ID основной игры: " + rs.getString(3)
                    + " | ID дополнения: " + rs.getString(1));
        }
    }

    public static void sumHoursPlayedOnOnePlatform(Connection connection, String platform) throws SQLException {
        PreparedStatement statement = connection.prepareStatement("SELECT SUM(hours) FROM games WHERE platform = ?");
        statement.setString(1, platform);
        ResultSet rs = statement.executeQuery();
        System.out.print("Суммарно сыграно на " + platform + ": ");
        while(rs.next()) {
            System.out.println(rs.getString(1) + " часов");
        }
    }

    public static void getGamesOrderByHoursPlayedDesc(Connection connection) throws SQLException {
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM games ORDER BY hours DESC");
        ResultSet rs = statement.executeQuery();
        System.out.println("По количеству сыгранных часов:");
        int count = 0;
        while(rs.next()) {
            count += 1;
            System.out.println(count + ". " + rs.getString(1) + " | Часов сыграно: " + rs.getString(4));
        }
    }

    public static void gamesMotherland(Connection connection) throws SQLException {
        PreparedStatement statement = connection.prepareStatement
                ("SELECT games.game_name, developers.dev_country FROM games LEFT JOIN developers ON games.dev = developers.dev_name");
        ResultSet rs = statement.executeQuery();
        int count = 0;
        while(rs.next()) {
            count += 1;
            System.out.println(count + ". " + rs.getString(1) + " | Офис: " + rs.getString(2));
        }
    }

    public static void baseGameDLC(Connection connection) throws SQLException {
        PreparedStatement statement = connection.prepareStatement
                ("SELECT games.game_name, dlc.dlc_name FROM games RIGHT JOIN dlc ON games.id = dlc.base_game_id");
        ResultSet rs = statement.executeQuery();
        int count = 0;
        while(rs.next()) {
            count += 1;
            System.out.println(count + ". " + rs.getString(1) + " | DLC: " + rs.getString(2));
        }
    }

    public static void gamesFromOneDev(Connection connection, String dev) throws SQLException {
        if (dev.isBlank()) {
            return;
        }
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM games WHERE dev = ?");
        statement.setString(1, dev);
        ResultSet rs = statement.executeQuery();
        System.out.println("Игры от " + dev + ":");
        int count = 0;
        while(rs.next()) {
            count += 1;
            System.out.println(count + ". " + rs.getString(1) + " | Год выхода: " + rs.getString(3)
                    + " | Часов сыграно: " + rs.getString(4) + " | Платформа: " + rs.getString(5)
                    + " | ID: " + rs.getString(6));
        }
    }

    public static void gamesOnOnePlatform(Connection connection, String platform) throws SQLException {
        if (platform.isBlank()) {
            return;
        }
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM games WHERE platform = ?");
        statement.setString(1, platform);
        ResultSet rs = statement.executeQuery();
        System.out.println("Игры на " + platform + ":");
        int count = 0;
        while(rs.next()) {
            count += 1;
            System.out.println(count + ". " + rs.getString(1) + " | Разработчик: " + rs.getString(2)
                    + " | Год выхода: " + rs.getString(3) + " | Часов сыграно: " + rs.getString(4)
                    + " | ID: " + rs.getString(6));
        }
    }

    public static void gameSearch(Connection connection, String game_name) throws SQLException {
        if (game_name.isBlank()) {
            return;
        }
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM games WHERE game_name = ?");
        statement.setString(1, game_name);
        ResultSet rs = statement.executeQuery();
        System.out.println("Вот, что удалось найти:");
        int count = 0;
        while(rs.next()) {
            count += 1;
            System.out.println(count + ". " + rs.getString(1) + " | Разработчик: " + rs.getString(2)
                    + " | Год выхода: " + rs.getString(3) + " | Часов сыграно: " + rs.getString(4)
                    + " | Платформа: " + rs.getString(5) + " | ID: " + rs.getString(6));
        }
    }

    public static void addGame(Connection connection, String game_name, String dev, int year, int hours, String platform)
            throws SQLException {
        if (year < 1961 || hours < 0 || game_name.isBlank() || dev.isBlank() || platform.isBlank()) {
            return;
        }
        PreparedStatement statement = connection.prepareStatement("INSERT INTO games(game_name, dev, year, hours, platform) " +
                "VALUES (?, ?, ?, ?, ?) returning id", Statement.RETURN_GENERATED_KEYS);
        statement.setString(1, game_name);
        statement.setString(2, dev);
        statement.setInt(3, year);
        statement.setInt(4, hours);
        statement.setString(5, platform);
        int count = statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк
        ResultSet rs = statement.getGeneratedKeys(); // прочитать запрошенные данные от БД
        if (rs.next()) { // прокрутить к первой записи, если они есть
            System.out.println();
            System.out.println("Идентификатор игры: " + rs.getInt(1));
        }

        System.out.println("Added " + count + " games");
        getGames(connection);
    }

    public static void addDeveloper(Connection connection, String dev_name, String dev_country) throws SQLException {
        if (dev_name.isBlank()) {
            return;
        }
        PreparedStatement statement = connection.prepareStatement("INSERT INTO developers(dev_name, dev_country) " +
                "VALUES (?, ?) returning dev_id", Statement.RETURN_GENERATED_KEYS);
        statement.setString(1, dev_name);
        statement.setString(2, dev_country);
        int count = statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк
        ResultSet rs = statement.getGeneratedKeys(); // прочитать запрошенные данные от БД
        if (rs.next()) { // прокрутить к первой записи, если они есть
            System.out.println();
            System.out.println("Идентификатор компании: " + rs.getInt(1));
        }
        System.out.println("Added " + count + " developers");
        getDevelopers(connection);
    }

    public static void addDLC(Connection connection, String dlc_name, int base_game_id) throws SQLException {
        if (dlc_name.isBlank() || base_game_id < 1) {
            return;
        }
        PreparedStatement statement = connection.prepareStatement("INSERT INTO dlc(dlc_name, base_game_id)" +
                "VALUES (?, ?) returning dlc_id", Statement.RETURN_GENERATED_KEYS);
        statement.setString(1, dlc_name);
        statement.setInt(2, base_game_id);
        int count = statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк
        ResultSet rs = statement.getGeneratedKeys(); // прочитать запрошенные данные от БД
        if (rs.next()) { // прокрутить к первой записи, если они есть
            System.out.println();
            System.out.println("Идентификатор дополнения: " + rs.getInt(1));
        }
        System.out.println("Added " + count + " DLCs");
        getDLC(connection);
    }

    private static void correctHoursPlayed(Connection connection, String game_name, int hours) throws SQLException {
        if (game_name.isBlank() || hours < 0) {
            return;
        }
        PreparedStatement statement = connection.prepareStatement("UPDATE games SET hours = ? WHERE game_name = ?");
        statement.setString(2, game_name);
        statement.setInt(1, hours);
        int count = statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк
        System.out.println("Updated " + count + " games");
        getGames(connection);
    }

    private static void deleteGame(Connection connection, int id) throws SQLException {
        if (id < 1) {
            return;
        }
        PreparedStatement statement = connection.prepareStatement("DELETE FROM games WHERE id = ?");
        statement.setInt(1, id);
        int count = statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк
        System.out.println("Deleted " + count + " games");
        getGames(connection);
    }

    private static void deleteDeveloper(Connection connection, int dev_id) throws SQLException {
        if (dev_id < 1) {
            return;
        }
        PreparedStatement statement = connection.prepareStatement("DELETE FROM developers WHERE dev_id = ?");
        statement.setInt(1, dev_id);
        int count = statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк
        System.out.println("Deleted " + count + " developers");
        getDevelopers(connection);
    }

    public static void deleteDLC(Connection connection, int dlc_id) throws SQLException {
        if (dlc_id < 1) {
            return;
        }
        PreparedStatement statement = connection.prepareStatement("DELETE FROM dlc WHERE dlc_id = ?");
        statement.setInt(1, dlc_id);
        int count = statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк
        System.out.println("Deleted " + count + " DLCs");
        getDLC(connection);
    }
}