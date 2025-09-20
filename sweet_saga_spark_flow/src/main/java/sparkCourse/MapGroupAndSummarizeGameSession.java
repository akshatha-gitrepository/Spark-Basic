package sparkCourse;

import com.daimler.ca.spark.flow.application.IOperationContext;
import com.daimler.ca.spark.flow.operations.OperationException;
import com.daimler.ca.spark.flow.operations.Transformation1;
import com.daimler.ca.spark.flow.operations.WireInfo;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import schema.JsonSchema;
import utils.GameEvent;
import utils.GameEventFactory;
import utils.GameSession;
import utils.SessionSummary;

public class MapGroupAndSummarizeGameSession extends Transformation1<Dataset<JsonSchema>, Dataset<SessionSummary>> {

    public MapGroupAndSummarizeGameSession(IOperationContext context, WireInfo<?> in, WireInfo<?> out) {
        super(context, in, out);
    }

    @Override
    protected Dataset<SessionSummary> work(Dataset<JsonSchema> data) {

        // Step 1: Map each row to GameEvent
        Dataset<GameEvent> events = data.toDF().map(
                (MapFunction<Row, GameEvent>) GameEventFactory::fromRow,
                Encoders.javaSerialization(GameEvent.class)
        );

        // Step 2: Group by (playerId, levelId)
        KeyValueGroupedDataset<String, GameEvent> grouped = events.groupByKey(
                (MapFunction<GameEvent, String>) event -> event.getPlayerId() + "," + event.getLevelId(),
                Encoders.STRING()
        );

        // Step 3: For each group, apply events to a GameSession and collect metrics

        // Order by playerId and levelId using column references instead of toDF conversion
        return grouped.mapGroups(
                (MapGroupsFunction<String, GameEvent, SessionSummary>) (key, eventsIter) -> {
                    String[] parts = key.split(",");
                    String playerId = parts[0];
                    Long levelId = Long.parseLong(parts[1]);
                    GameSession session = new GameSession();
                    session.setPlayerId(playerId);
                    session.setLevelId(levelId);
                    while (eventsIter.hasNext()) {
                        GameEvent event = eventsIter.next();
                        event.apply(session);
                    }
                    return SessionSummary.builder()
                            .playerId(playerId)
                            .levelId(levelId)
                            .moves(session.getMoves())
                            .score(session.getScore())
                            .completed(session.isCompleted())
                            .boosters(session.getBoosters())
                            .efficiency(session.getEfficiency())
                            .build();
                },
                Encoders.bean(SessionSummary.class)
        );
    }
}