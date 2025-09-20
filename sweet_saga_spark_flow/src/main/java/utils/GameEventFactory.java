package utils;

import events.*;
import org.apache.spark.sql.Row;

public class GameEventFactory {

    public static GameEvent fromRow(Row row) {
        // Access fields by name using the defined schema
        String playerId = row.getAs("playerId");
        String eventType = row.getAs("eventType");
        Long levelId = row.getAs("levelId");
        Row eventData = row.getAs("eventData");  // Now properly accessing by field name

        switch (eventType) {
            case "SWAP":
                String colorA = getStringFromEventData(eventData, "colorA");
                String colorB = getStringFromEventData(eventData, "colorB");
                return new SwapEvent(playerId, colorA, colorB, levelId);
            case "BOOSTER_USED":
                String boosterType = getStringFromEventData(eventData, "type");
                return new BoosterUsedEvent(playerId, boosterType, levelId);
            case "LEVEL_COMPLETE":
                String score = getStringFromEventData(eventData, "score");
                return new LevelCompleteEvent(playerId, levelId, score);
            case "LEVEL_FAIL":
                return new LevelFailEvent(playerId, levelId);
            case "LIFE_LOST":
                return new LifeLostEvent(playerId, levelId);
            default:
                throw new IllegalArgumentException("Unknown eventType: " + eventType);
        }
    }

    // Updated helper method to access eventData fields by name instead of index
    private static String getStringFromEventData(Row eventData, String fieldName) {
        if (eventData == null || eventData.isNullAt(eventData.fieldIndex(fieldName))) {
            return null;
        }
        return eventData.getAs(fieldName);
    }
}
