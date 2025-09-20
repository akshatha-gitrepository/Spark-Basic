package events;

import utils.GameEvent;
import utils.GameSession;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class BoosterUsedEvent extends GameEvent implements Serializable {

    private String playerId;
    private String boosterType;
    private Long levelId;

    @Override
    public void apply(GameSession session) {
        session.incrementBoosters();
        session.addScore(500);
    }
}
