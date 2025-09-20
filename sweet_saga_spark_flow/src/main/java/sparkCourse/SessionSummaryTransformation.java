package sparkCourse;

import com.daimler.ca.spark.flow.application.IOperationContext;
import com.daimler.ca.spark.flow.operations.OperationException;
import com.daimler.ca.spark.flow.operations.Transformation1;
import com.daimler.ca.spark.flow.operations.WireInfo;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import utils.GameEvent;
import utils.GameSession;
import utils.SessionSummary;

import java.util.Iterator;

public class SessionSummaryTransformation extends Transformation1<Dataset<GameEvent>, Dataset<SessionSummary>> {
    public SessionSummaryTransformation(IOperationContext context, WireInfo<?> i1, WireInfo<?> out) {
        super(context, i1, out);
    }

    /**
     * @param gameEventDataset
     * @return
     * @throws OperationException
     */
    @Override
    protected Dataset<SessionSummary> work(Dataset<GameEvent> gameEventDataset) throws OperationException {
        KeyValueGroupedDataset<String, GameEvent> grouped = gameEventDataset.groupByKey(
                (MapFunction<GameEvent, String>) event -> event.getPlayerId() + "," + event.getLevelId(),
                Encoders.STRING()
        );

        // Process each group and create session summaries
        return grouped.mapGroups(
                this::processEventGroup,
                Encoders.bean(SessionSummary.class)
        );
    }

    /**
     * Processes a group of game events and produces a session summary
     */
    private SessionSummary processEventGroup(String key, Iterator<GameEvent> eventsIter) {
        // Parse the composite key
        String[] parts = key.split(",");
        String playerId = parts[0];
        Long levelId = Long.parseLong(parts[1]);

        // Initialize and populate the session
        GameSession session = new GameSession();
        session.setPlayerId(playerId);
        session.setLevelId(levelId);

        while (eventsIter.hasNext()) {
            GameEvent event = eventsIter.next();
            event.apply(session);
        }

        // Build summary from session state
        return SessionSummary.builder()
                .playerId(playerId)
                .levelId(levelId)
                .moves(session.getMoves())
                .score(session.getScore())
                .completed(session.isCompleted())
                .boosters(session.getBoosters())
                .efficiency(session.getEfficiency())
                .build();
    }
}
