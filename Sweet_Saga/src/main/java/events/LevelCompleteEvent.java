package events;

import lombok.AllArgsConstructor;
import lombok.Data;
import utils.GameEvent;
import utils.GameSession;


import java.io.Serializable;

@Data
@AllArgsConstructor
public class LevelCompleteEvent extends GameEvent implements Serializable {

    private String playerId;
    private Long levelId;
    private String score;

    @Override
    public void apply(GameSession session) {
        session.setCompleted(true);
        if (score != null) {
            session.setCompletedScore(Integer.parseInt(score));
        }
    }
}
