package hu.dbx.kompot.moby;

import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.producer.EventGroupProvider;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * A database-drive implementation.
 * Reads event groups from a table called ct_event_subscribers.
 */
@SuppressWarnings("unused")
public final class MobyEventGroupProvider implements EventGroupProvider {

    private final Connection connection;

    private final static String QUERY = "SELECT module_code, event_code FROM ct_event_subscribers WHERE event_code = ?";

    public MobyEventGroupProvider(Connection connection) {
        this.connection = connection;
    }

    // TODO: test this class!

    @Override
    public Iterable<String> findEventGroups(EventDescriptor marker) {
        try {
            List<String> output = new LinkedList<>();
            ResultSet result = connection.prepareStatement(QUERY, new String[]{marker.getEventName()}).executeQuery();
            while (result.next()) {
                output.add(result.getString("module_code"));
            }
            return output;
        } catch (SQLException e) {
            throw new IllegalStateException("SQL error. Could not fetch configuration.", e);
        }
    }
}