package events;

import utils.GameEvent;
import utils.GameSession;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class LevelFailEvent extends GameEvent implements Serializable {

    private String playerId;
    private Long levelId;

    @Override
    public void apply(GameSession session) {
        session.setCompleted(false);
        session.addScore(0);
    }
}
